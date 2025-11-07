<# 
.SYNOPSIS
  Pull /fills and compute realized P&L by FIFO, grouped by strategy and symbol.

.DESCRIPTION
  - Calls /health, optionally /journal/sync, then fetches /fills (paged).
  - Heuristically maps fields: side(buy/sell), price, qty, fee, symbol, strategy, timestamp.
  - Runs FIFO matching per (strategy, symbol) to compute realized P&L:
      Realized = (sell_proceeds - matched_buy_cost) - fees_on_both_sides
  - Outputs two files:
      reports/fifo_ledger_YYYYMMDD_HHMMSS.csv   (per matched lot)
      reports/pnl_summary_YYYYMMDD_HHMMSS.csv   (per strategy + symbol and totals)
  - Prints summary table to console.

.PARAMETER BaseUrl
  API base (e.g., https://crypto-system-z605.onrender.com)

.PARAMETER OutDir
  Where to write CSV/JSON (default ./reports)

.PARAMETER SinceHours
  Rolling lookback window; client-filters by fill timestamp when present.

.PARAMETER Limit
  Page size for /fills (default 1000)

.PARAMETER SyncJournal
  If set, calls POST /journal/sync first.

.PARAMETER ApiKey
  Optional bearer token for Authorization header.

.PARAMETER DrySymbols
  Comma-separated symbol allowlist (e.g., "BTC/USD,ETH/USD"). If omitted, uses all.

.PARAMETER Strategies
  Comma-separated strategy allowlist (e.g., "c1,c2"). If omitted, uses all.

#>

param(
  [Parameter(Mandatory=$true)][string]$BaseUrl,
  [string]$OutDir = "$(Join-Path -Path (Get-Location) -ChildPath 'reports')",
  [int]$SinceHours = 24,
  [int]$Limit = 1000,
  [switch]$SyncJournal,
  [string]$ApiKey,
  [string]$DrySymbols,
  [string]$Strategies
)

# ----------------- helpers -----------------
function New-AuthHeader { param([string]$ApiKey)
  if ([string]::IsNullOrWhiteSpace($ApiKey)) { return @{} }
  @{ Authorization = "Bearer $ApiKey" }
}
$CommonHeaders = @{ "Accept"="application/json" } + (New-AuthHeader -ApiKey $ApiKey)

function Invoke-Api {
  param([string]$Path,[string]$Method="GET",[hashtable]$Query=@{},[object]$Body=$null,[int]$TimeoutSec=90)
  $ub = [System.UriBuilder]::new($BaseUrl.TrimEnd('/')); $ub.Path = ($ub.Path.TrimEnd('/') + '/' + $Path.TrimStart('/'))
  if ($Query.Count -gt 0) {
    $q = [System.Web.HttpUtility]::ParseQueryString([string]::Empty)
    foreach ($k in $Query.Keys) { if ($Query[$k] -ne $null -and $Query[$k] -ne "") { $q[$k] = [string]$Query[$k] } }
    $ub.Query = $q.ToString()
  }
  $u = $ub.Uri.AbsoluteUri
  try {
    if ($Method -eq "GET") { return Invoke-RestMethod -Uri $u -Headers $CommonHeaders -TimeoutSec $TimeoutSec }
    elseif ($Method -eq "POST") {
      if ($Body -ne $null) {
        return Invoke-RestMethod -Method POST -Uri $u -Headers ($CommonHeaders + @{ "Content-Type"="application/json"}) -Body ($Body | ConvertTo-Json -Depth 10) -TimeoutSec $TimeoutSec
      } else {
        return Invoke-RestMethod -Method POST -Uri $u -Headers $CommonHeaders -TimeoutSec $TimeoutSec
      }
    } else { throw "Unsupported method $Method" }
  } catch { Write-Warning "Invoke-Api $Method $u failed: $($_.Exception.Message)"; return $null }
}

function Ensure-OutDir { if (-not (Test-Path -LiteralPath $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null } }

# Heuristic getters for fill fields
function Get-FieldValue {
  param($obj,[string[]]$candidates)
  foreach ($c in $candidates) {
    if ($obj.PSObject.Properties.Name -contains $c -and $obj.$c -ne $null -and $obj.$c -ne "") { return $obj.$c }
  }
  return $null
}

function Parse-Side { param([string]$sideRaw)
  if (!$sideRaw) { return $null }
  $s = $sideRaw.ToString().ToLowerInvariant()
  if ($s -match "buy|long") { return "buy" }
  if ($s -match "sell|short|close") { return "sell" }
  return $null
}

# ----------------- start -----------------
Write-Host "== P&L FIFO check @ $BaseUrl =="
$sinceUtc = [DateTimeOffset]::UtcNow.AddHours(-$SinceHours)
Write-Host ("Window: last {0}h (since {1:u})" -f $SinceHours, $sinceUtc)

$h = Invoke-Api -Path "health"
if ($h) { Write-Host "[OK] /health responded." } else { Write-Warning "[WARN] /health missing." }

if ($SyncJournal) {
  $js = Invoke-Api -Path "journal/sync" -Method "POST"
  if ($js) { Write-Host "[OK] journal synced." } else { Write-Warning "[WARN] journal sync failed (continuing)." }
}

# Pull fills (paged)
Write-Host "Fetching /fills…"
$all = @(); $offset = 0; $more=$true
while ($more) {
  $page = Invoke-Api -Path "fills" -Query @{ limit=$Limit; offset=$offset }
  if (!$page) { if ($offset -eq 0) { break } else { $more=$false; break } }
  if ($page -is [System.Collections.IEnumerable]) {
    $count = 0; foreach ($r in $page) { $all += $r; $count++ }
    if ($count -lt $Limit) { $more=$false } else { $offset += $Limit }
  } elseif ($page.PSObject.Properties.Name -contains "items") {
    $items = @($page.items); $all += $items
    if ($items.Count -lt $Limit) { $more=$false } else { $offset += $Limit }
  } else { $all = @($page); $more=$false }
  if ($offset -gt 100000) { $more=$false } # safety
}

if ($all.Count -eq 0) { Write-Warning "No fills returned."; exit }

# Normalize & filter time/symbol/strategy
$fills = @()
foreach ($o in $all) {
  # timestamp
  $ts = Get-FieldValue $o @("timestamp","time","created_at","fill_time","ts")
  try { $dt = if ($ts) { [DateTimeOffset]$ts } else { $null } } catch { $dt = $null }
  # symbol
  $sym = Get-FieldValue $o @("symbol","pair","market","asset","instrument")
  # strategy
  $strat = Get-FieldValue $o @("strategy","strat","system","name","s")
  if (-not $strat) { $strat = "(unknown)" }
  # side
  $sideRaw = Get-FieldValue $o @("side","action","direction")
  $side = Parse-Side $sideRaw
  # price
  $price = Get-FieldValue $o @("price","avg_price","p")
  try { $px = if ($price -ne $null) { [double]$price } else { $null } } catch { $px = $null }
  # quantity
  $qtyRaw = Get-FieldValue $o @("qty","quantity","volume","size","q","amount")
  try { $qty = if ($qtyRaw -ne $null) { [double]$qtyRaw } else { $null } } catch { $qty = $null }
  # fee
  $feeRaw = Get-FieldValue $o @("fee","fees","commission","cost_fee")
  try { $fee = if ($feeRaw -ne $null) { [double]$feeRaw } else { 0.0 } } catch { $fee = 0.0 }

  if ($dt -and $dt -lt $sinceUtc) { continue }
  if ($DrySymbols) {
    $allow = $DrySymbols.Split(",") | ForEach-Object { $_.Trim() }
    if ($sym -and -not ($allow -contains $sym)) { continue }
  }
  if ($Strategies) {
    $allowS = $Strategies.Split(",") | ForEach-Object { $_.Trim() }
    if (-not ($allowS -contains $strat)) { continue }
  }

  $fills += [pscustomobject]@{
    dt=$dt; strategy=$strat; symbol=$sym; side=$side; price=$px; qty=$qty; fee=$fee; raw=$o
  }
}

if ($fills.Count -eq 0) { Write-Warning "No fills in the time window/filters."; exit }

# Group per (strategy, symbol) and run FIFO
$fifoLedger = @()
$summary = @()

$groups = $fills | Where-Object { $_.side -and $_.price -ne $null -and $_.qty -ne $null -and $_.qty -ne 0 } |
          Sort-Object dt |
          Group-Object strategy, symbol

foreach ($g in $groups) {
  $parts = $g.Name -split ", "
  $strat = $parts[0].Split("=")[-1]
  $sym   = $parts[1].Split("=")[-1]

  # open lots: queue of [qty_remaining, avg_price, fee_accum]
  $open = New-Object System.Collections.Generic.Queue[object]
  $realized = 0.0

  foreach ($f in ($g.Group | Sort-Object dt)) {
    $qty = [double]$f.qty
    $px  = [double]$f.price
    $fee = [double]$f.fee
    if ($f.side -eq "buy") {
      # push as a new lot
      $open.Enqueue(@{ qty=$qty; price=$px; fee=$fee })
    } elseif ($f.side -eq "sell") {
      $remaining = $qty
      $sellProceeds = 0.0  # track matched lot proceeds for ledger entries
      while ($remaining -gt 1e-12 -and $open.Count -gt 0) {
        $lot = $open.Dequeue()
        $take = [Math]::Min($remaining, [double]$lot.qty)
        $cost = $take * [double]$lot.price
        $proceeds = $take * $px
        # pro-rate fees: buy side portion
        $buyFeePro = ([double]$lot.fee) * ($take / [double]$lot.qty)
        # sell fee pro-rate by quantity
        $sellFeePro = $fee * ($take / $qty)
        $pnl = ($proceeds - $cost) - ($buyFeePro + $sellFeePro)
        $realized += $pnl
        $sellProceeds += $proceeds

        # record ledger row
        $fifoLedger += [pscustomobject]@{
          dt         = $f.dt
          strategy   = $strat
          symbol     = $sym
          sell_px    = [math]::Round($px, 8)
          buy_px     = [math]::Round([double]$lot.price, 8)
          qty        = [math]::Round($take, 8)
          proceeds   = [math]::Round($proceeds, 8)
          cost       = [math]::Round($cost, 8)
          fee_buy    = [math]::Round($buyFeePro, 8)
          fee_sell   = [math]::Round($sellFeePro, 8)
          pnl        = [math]::Round($pnl, 8)
        }

        $remaining -= $take
        $lot.qty = [double]$lot.qty - $take
        if ($lot.qty -gt 1e-12) { $open.Enqueue($lot) } # put back leftover
      }
      # (If remaining > 0 and no open buys left, we assume flat-to-short isn’t supported and ignore residual.)
    }
  }

  $summary += [pscustomobject]@{
    strategy = $strat
    symbol   = $sym
    realized_pnl = [math]::Round($realized, 8)
    source   = "fills_fifo"
  }
}

if ($summary.Count -eq 0) {
  Write-Warning "No realized P&L computed (no sell events matched to prior buys)."
  exit
}

# Roll up per-strategy and totals
$byStrat = $summary | Group-Object strategy | ForEach-Object {
  [pscustomobject]@{
    strategy = $_.Name
    pnl      = [math]::Round( ($_.Group | Measure-Object -Property realized_pnl -Sum).Sum, 8 )
  }
}
$total = [math]::Round( ($byStrat | Measure-Object -Property pnl -Sum).Sum, 8 )

# --------------- save & print ---------------
Ensure-OutDir
$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$ledgerCsv = Join-Path $OutDir "fifo_ledger_${stamp}.csv"
$sumCsv    = Join-Path $OutDir "pnl_summary_${stamp}.csv"
$sumJson   = Join-Path $OutDir "pnl_summary_${stamp}.json"

try { $fifoLedger | Export-Csv -NoTypeInformation -Path $ledgerCsv; Write-Host "[Saved] $ledgerCsv" } catch { Write-Warning "Failed to save ledger CSV: $($_.Exception.Message)" }
try { $summary    | Export-Csv -NoTypeInformation -Path $sumCsv;    Write-Host "[Saved] $sumCsv" } catch { Write-Warning "Failed to save summary CSV: $($_.Exception.Message)" }

# console table
Write-Host ""
Write-Host "P&L by Strategy:"
$byStrat | Sort-Object strategy | Format-Table -AutoSize
Write-Host ("TOTAL P&L: {0}" -f $total)

# JSON bundle
try {
  $bundle = [pscustomobject]@{
    base_url  = $BaseUrl
    since_utc = $sinceUtc.ToString("o")
    generated = (Get-Date).ToString("o")
    total_pnl = $total
    by_strategy = $byStrat
    by_strategy_symbol = $summary
  }
  $bundle | ConvertTo-Json -Depth 8 | Out-File -FilePath $sumJson -Encoding UTF8
  Write-Host "[Saved] $sumJson"
} catch { Write-Warning "Failed to save summary JSON: $($_.Exception.Message)" }
