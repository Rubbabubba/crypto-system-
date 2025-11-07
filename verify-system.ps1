<#
.SYNOPSIS
  Verify crypto-system is respecting whitelist, windows/guard, and strategies.

.DESCRIPTION
  - Pulls /scheduler/status (if available).
  - Triggers POST /scheduler/run with a DRY-RUN plan (no orders).
  - Compares the server’s echoed/returned plan to the expected inputs.
  - Prints PASS/FAIL for: symbols (whitelist), strategies, timeframe, limit, notional, guard/window.
  - Saves request/response to ./reports/verify_<timestamp>.json for audit.

.PARAMETER BaseUrl
  Base URL of your API (ex: https://crypto-system-z605.onrender.com)

.PARAMETER Whitelist
  Comma-separated symbols (e.g., "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD")

.PARAMETER Strategies
  Comma-separated strategy keys (e.g., "c1,c2,c3,c4,c5,c6")

.PARAMETER Timeframe
  Trading timeframe string (e.g., "5Min")

.PARAMETER Limit
  Candle/history limit to request (e.g., 300)

.PARAMETER Notional
  Per-trade notional (e.g., 25)

.PARAMETER RequireGuard
  If provided, assert guard/window is ON (true). If missing, do not assert.

.PARAMETER Window
  Optional window label to assert (e.g., "live")

.PARAMETER DryRun
  If provided, force dry run (recommended). Defaults to dry run if omitted.

.PARAMETER ApiKey
  Optional Authorization bearer token.

.EXAMPLE
  .\verify-system.ps1 `
    -BaseUrl https://crypto-system-z605.onrender.com `
    -Whitelist "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD" `
    -Strategies "c1,c2,c3,c4,c5,c6" `
    -Timeframe "5Min" -Limit 300 -Notional 25 -RequireGuard -Window "live" -DryRun
#>

param(
  [Parameter(Mandatory=$true)] [string]$BaseUrl,
  [Parameter(Mandatory=$true)] [string]$Whitelist,
  [Parameter(Mandatory=$true)] [string]$Strategies,
  [string]$Timeframe = "5Min",
  [int]$Limit = 300,
  [double]$Notional = 25.0,
  [switch]$RequireGuard,
  [string]$Window,
  [switch]$DryRun,
  [string]$ApiKey,
  [string]$OutDir = "$(Join-Path -Path (Get-Location) -ChildPath 'reports')"
)

# --- Helpers ---------------------------------------------------------------
function New-AuthHeader {
  param([string]$ApiKey)
  if ([string]::IsNullOrWhiteSpace($ApiKey)) { return @{} }
  return @{ Authorization = "Bearer $ApiKey" }
}
$CommonHeaders = @{ "Accept" = "application/json" } + (New-AuthHeader -ApiKey $ApiKey)

function Invoke-Api {
  param(
    [string]$Path,
    [string]$Method = "GET",
    [hashtable]$Query = @{},
    [object]$Body = $null,
    [int]$TimeoutSec = 60
  )
  $ub = [System.UriBuilder]::new($BaseUrl.TrimEnd('/'))
  $ub.Path = ($ub.Path.TrimEnd('/') + '/' + $Path.TrimStart('/'))
  if ($Query.Count -gt 0) {
    $q = [System.Web.HttpUtility]::ParseQueryString([string]::Empty)
    foreach ($k in $Query.Keys) { if ($Query[$k] -ne $null -and $Query[$k] -ne "") { $q[$k] = [string]$Query[$k] } }
    $ub.Query = $q.ToString()
  }
  $uri = $ub.Uri.AbsoluteUri
  try {
    if ($Method -eq "GET") {
      return Invoke-RestMethod -Method GET -Uri $uri -Headers $CommonHeaders -TimeoutSec $TimeoutSec
    } elseif ($Method -eq "POST") {
      if ($Body -ne $null) {
        return Invoke-RestMethod -Method POST -Uri $uri -Headers ($CommonHeaders + @{ "Content-Type"="application/json"}) -Body ($Body | ConvertTo-Json -Depth 12) -TimeoutSec $TimeoutSec
      } else {
        return Invoke-RestMethod -Method POST -Uri $uri -Headers $CommonHeaders -TimeoutSec $TimeoutSec
      }
    } else {
      throw "Unsupported method: $Method"
    }
  } catch {
    Write-Warning "Invoke-Api $Method $uri failed: $($_.Exception.Message)"
    return $null
  }
}

function Ensure-OutDir { if (-not (Test-Path -LiteralPath $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null } }

function As-Set([System.Collections.IEnumerable]$items) {
  return ($items | Where-Object { $_ -ne $null -and $_ -ne "" } | ForEach-Object { $_.ToString().Trim() } | Sort-Object -Unique)
}

function Print-Check($name, $ok, $expected, $actual) {
  $status = if ($ok) { "PASS" } else { "FAIL" }
  Write-Host ("[{0}] {1}" -f $status, $name)
  if (-not $ok) {
    Write-Host ("  expected: {0}" -f ($expected -join ", "))
    Write-Host ("  actual  : {0}" -f ($actual -join ", "))
  }
}

# --- Expected inputs (normalize) ------------------------------------------
$expSymbols   = As-Set ($Whitelist -split ",")
$expStrats    = As-Set ($Strategies -split ",")
$expTf        = $Timeframe
$expLimit     = [int]$Limit
$expNotional  = [double]$Notional
$wantGuard    = [bool]$RequireGuard
$wantWindow   = if ($Window) { $Window.Trim() } else { $null }

Write-Host "== Verifying config on $BaseUrl =="
Write-Host "Expect Symbols : $($expSymbols -join ', ')"
Write-Host "Expect Strats  : $($expStrats -join ', ')"
Write-Host "Expect TF/LM/N : $expTf / $expLimit / $expNotional"
if ($RequireGuard) { Write-Host "Expect Guard   : ON" }
if ($wantWindow)   { Write-Host "Expect Window  : $wantWindow" }

# --- Peek at status (best-effort) -----------------------------------------
$status = Invoke-Api -Path "scheduler/status"
if ($status -ne $null) {
  $ticksCount = 0
  if ($status.PSObject.Properties.Name -contains "ticks") {
    $ticksRaw = @($status.ticks | ForEach-Object { $_ })
    $ticksCount = $ticksRaw.Count
  }
  Write-Host ("Status: enabled={0} interval={1}s ticks={2}" -f $status.enabled, $status.interval_secs, $ticksCount)
}

# --- Build dry-run plan ----------------------------------------------------
# Keys match your server logs: "strats=..., tf=..., limit=..., notional=..., dry=..., symbols=..."
$body = @{
  strats   = $expStrats
  tf       = $expTf
  limit    = $expLimit
  notional = $expNotional
  dry      = $true
  symbols  = $expSymbols
}
# Optionally assert guard/window if server accepts them; harmless if ignored.
if ($RequireGuard) { $body.guard = $true }
if ($wantWindow)   { $body.window = $wantWindow }

Write-Host "Triggering dry-run /scheduler/run..."
$response = Invoke-Api -Path "scheduler/run" -Method "POST" -Body $body

# Save request+response for audit
Ensure-OutDir
$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$savePath = Join-Path $OutDir "verify_${stamp}.json"
@{
  base_url = $BaseUrl
  requested = $body
  status    = $status
  response  = $response
  generated = (Get-Date).ToString("o")
} | ConvertTo-Json -Depth 12 | Out-File -FilePath $savePath -Encoding UTF8
Write-Host "[Saved] $savePath"

# --- Extract what the server says it USED ---------------------------------
# We try several common shapes:
# 1) response.plan.{symbols,strats,tf,limit,notional,guard,window}
# 2) response.{symbols,strats,tf,limit,notional,guard,window}
# 3) status.{symbols,strats,tf,limit,notional,guard,window}
function pick($obj, $path) {
  if (-not $obj) { return $null }
  $cur = $obj
  foreach ($seg in $path -split "\.") {
    if ($cur -eq $null) { return $null }
    if ($cur.PSObject.Properties.Name -contains $seg) {
      $cur = $cur.$seg
    } else {
      return $null
    }
  }
  return $cur
}

$actSymbols  = As-Set @( (pick $response "plan.symbols"), (pick $response "symbols"), (pick $status "symbols") )
$actStrats   = As-Set @( (pick $response "plan.strats"),  (pick $response "strats"),  (pick $status "strats") )
$actTf       = (pick $response "plan.tf");       if (-not $actTf)      { $actTf      = (pick $response "tf")      }
if (-not $actTf) { $actTf = (pick $status "tf") }

$actLimit    = (pick $response "plan.limit");    if (-not $actLimit)   { $actLimit   = (pick $response "limit")   }
if (-not $actLimit) { $actLimit = (pick $status "limit") }

$actNotional = (pick $response "plan.notional"); if (-not $actNotional){ $actNotional= (pick $response "notional")}
if (-not $actNotional) { $actNotional = (pick $status "notional") }

$actGuard    = (pick $response "plan.guard");    if ($null -eq $actGuard)   { $actGuard   = (pick $response "guard")   }
if ($null -eq $actGuard) { $actGuard = (pick $status "guard") }

$actWindow   = (pick $response "plan.window");   if (-not $actWindow)  { $actWindow  = (pick $response "window")  }
if (-not $actWindow) { $actWindow = (pick $status "window") }

# --- Assertions ------------------------------------------------------------
$okSymbols = ($actSymbols.Count -gt 0) -and (Compare-Object -ReferenceObject $expSymbols -DifferenceObject $actSymbols | Measure-Object).Count -eq 0
$okStrats  = ($actStrats.Count -gt 0)  -and (Compare-Object -ReferenceObject $expStrats  -DifferenceObject $actStrats  | Measure-Object).Count -eq 0
$okTf      = $actTf -and ($actTf.ToString() -eq $expTf)
$okLimit   = $actLimit -and ([int]$actLimit -eq $expLimit)
$okNotion  = $actNotional -and ([double]$actNotional -eq $expNotional)

$okGuard   = $true
if ($RequireGuard) {
  $okGuard = ($null -ne $actGuard) -and ([bool]$actGuard -eq $true)
}
$okWindow  = $true
if ($wantWindow) {
  $okWindow = $actWindow -and ($actWindow.ToString() -eq $wantWindow)
}

Write-Host ""
Print-Check "Whitelist (symbols)" $okSymbols $expSymbols $actSymbols
Print-Check "Strategies"          $okStrats  $expStrats  $actStrats
Print-Check "Timeframe"           $okTf      @($expTf)   @($actTf)
Print-Check "Limit"               $okLimit   @($expLimit) @($actLimit)
Print-Check "Notional"            $okNotion  @($expNotional) @($actNotional)
if ($RequireGuard) { Print-Check "Guard Enabled" $okGuard @("true") @("$actGuard") }
if ($wantWindow)   { Print-Check "Window"        $okWindow @($wantWindow) @("$actWindow") }

$allOk = $okSymbols -and $okStrats -and $okTf -and $okLimit -and $okNotion -and $okGuard -and $okWindow
Write-Host ""
if ($allOk) {
  Write-Host "[SUCCESS] Server plan matches expected config."
} else {
  Write-Warning "[MISMATCH] One or more checks failed. See $savePath for raw response."
}
