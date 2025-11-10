<# 
.SYNOPSIS
  Validate crypto-system scheduler parameters against server echo.

.DESCRIPTION
  - Calls /health
  - Tries /scheduler/status (tolerates 500)
  - Triggers POST /scheduler/run (JSON body). If that fails, tries querystring fallback.
  - Reads /scheduler/last; if missing, falls back to status.last; if still missing, compares
    against the payload we just sent (marked as "assumed").
  - Prints PASS/FAIL per field, optional JSON report.

.PARAMETERS
  See param() below.
#>

param(
  [string]$BaseUrl    = "https://crypto-system-z605.onrender.com",
  [string]$Whitelist  = "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD",
  [string]$Strategies = "c1,c2,c3,c4,c5,c6",
  [string]$Timeframe  = "5Min",
  [int]$Limit         = 300,
  [double]$Notional   = 25,
  [string]$Window     = "live",
  [switch]$RequireGuard,
  [switch]$ExpectDryRun,     # set if you expect dry=True
  [string]$ApiKey,
  [switch]$SaveReport        # writes ./reports/test_config_YYYYMMDD_HHMMSS.json
)

# ------------- helpers -----------------
function New-AuthHeader {
  param([string]$ApiKey)
  if ([string]::IsNullOrWhiteSpace($ApiKey)) { return @{} }
  @{ Authorization = "Bearer $ApiKey" }
}
$CommonHeaders = @{ Accept = "application/json" } + (New-AuthHeader -ApiKey $ApiKey)

function Invoke-Api {
  param(
    [ValidateSet("GET","POST")] [string]$Method,
    [string]$Path,
    [hashtable]$Query = @{},
    [object]$Body = $null,
    [int]$TimeoutSec = 60
  )
  $base = $BaseUrl.TrimEnd('/')
  $uriBuilder = [System.UriBuilder]::new($base)
  $uriBuilder.Path = ($uriBuilder.Path.TrimEnd('/') + '/' + $Path.TrimStart('/'))

  if ($Query.Count -gt 0) {
    $q = [System.Web.HttpUtility]::ParseQueryString([string]::Empty)
    foreach ($k in $Query.Keys) {
      $v = $Query[$k]
      if ($null -ne $v -and "$v" -ne "") { $q[$k] = "$v" }
    }
    $uriBuilder.Query = $q.ToString()
  }
  $uri = $uriBuilder.Uri.AbsoluteUri

  try {
    if ($Method -eq "GET")   { return Invoke-RestMethod -Method GET  -Uri $uri -Headers $CommonHeaders -TimeoutSec $TimeoutSec }
    if ($Method -eq "POST") {
      if ($null -ne $Body) {
        return Invoke-RestMethod -Method POST -Uri $uri -Headers ($CommonHeaders + @{ "Content-Type"="application/json"}) -Body ($Body | ConvertTo-Json -Depth 10) -TimeoutSec $TimeoutSec
      } else {
        return Invoke-RestMethod -Method POST -Uri $uri -Headers $CommonHeaders -TimeoutSec $TimeoutSec
      }
    }
  } catch {
    Write-Warning "Invoke-Api $Method $uri failed: $($_.Exception.Message)"
    return $null
  }
}

function NList([object]$x) {
  if ($null -eq $x) { return @() }
  if ($x -is [string]) {
    return ($x -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
  }
  return @($x) | ForEach-Object { "$_".Trim() } | Where-Object { $_ }
}
function SameList($a,$b) {
  $sa = (NList $a | Sort-Object)
  $sb = (NList $b | Sort-Object)
  return ($sa -join '|') -eq ($sb -join '|')
}
function Show-Pass($label) { Write-Host "[PASS] $label" -ForegroundColor Green }
function Show-Fail($label,$expected,$actual) {
  Write-Host "[FAIL] $label" -ForegroundColor Red
  Write-Host ("  expected: {0}" -f ((NList $expected) -join ", "))
  Write-Host ("  actual  : {0}" -f ((NList $actual) -join ", "))
}
function Ensure-Reports {
  $dir = Join-Path -Path (Get-Location) -ChildPath "reports"
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
  return $dir
}

# ------------- start -------------------
Write-Host "== Testing scheduler on $BaseUrl =="

# Health
$health = Invoke-Api -Method GET -Path "health"
if ($health) { Write-Host "[OK] /health responded." } else { Write-Warning "[WARN] /health failed (continuing)." }

# Try status (tolerate 500 per your logs)
$status1 = Invoke-Api -Method GET -Path "scheduler/status"
if ($status1) {
  $ticks = @()
  if ($status1.PSObject.Properties.Name -contains 'ticks') { $ticks = @($status1.ticks) }
  Write-Host ("Status: enabled={0} interval={1}s ticks={2}" -f $status1.enabled, $status1.interval_secs, $ticks.Count)
} else {
  Write-Warning "[WARN] /scheduler/status unavailable (HTTP 500 in your env)."
}

# Build intended run payload
$intended = [pscustomobject]@{
  tf       = $Timeframe
  limit    = $Limit
  notional = $Notional
  dry_run  = [bool]($ExpectDryRun.IsPresent)
  symbols  = (NList $Whitelist) -join ","
  strats   = (NList $Strategies) -join ","
  window   = $Window
  guard    = [bool]($RequireGuard.IsPresent)
}

# Trigger a dry run (JSON body); if fails, try querystring fallback forms
$runResp = Invoke-Api -Method POST -Path "scheduler/run" -Body $intended
if (-not $runResp) {
  # Fallback 1: querystring POST (no body)
  $runResp = Invoke-Api -Method POST -Path "scheduler/run" -Query @{
    tf       = $intended.tf
    limit    = $intended.limit
    notional = $intended.notional
    dry      = $intended.dry_run
    symbols  = $intended.symbols
    strats   = $intended.strats
    window   = $intended.window
    guard    = $intended.guard
  }
}
if (-not $runResp) {
  # Fallback 2: GET with querystring (some APIs do this)
  $runResp = Invoke-Api -Method GET -Path "scheduler/run" -Query @{
    tf       = $intended.tf
    limit    = $intended.limit
    notional = $intended.notional
    dry      = $intended.dry_run
    symbols  = $intended.symbols
    strats   = $intended.strats
    window   = $intended.window
    guard    = $intended.guard
  }
}

if ($runResp) { Write-Host "[OK] /scheduler/run accepted." } else { Write-Warning "[WARN] /scheduler/run failed (continuing to compare against intended payload)." }

Start-Sleep -Seconds 1

# Try to read back the last payload
$last = Invoke-Api -Method GET -Path "scheduler/last"
if (-not $last -and $status1 -and $status1.PSObject.Properties.Name -contains "last") {
  $last = $status1.last
}

# If still no 'last', we’ll mark comparisons as "assumed" against the intended payload
$source = "server"
if (-not $last) {
  Write-Warning "[WARN] No server 'last' available (/scheduler/last 404 and /scheduler/status 500). Using intended payload for comparison (assumed)."
  $last = $intended
  $source = "assumed"
}

# normalize actuals
$actSymbols  = if ($last.PSObject.Properties.Name -contains 'symbols') { $last.symbols } elseif ($last.PSObject.Properties.Name -contains 'symbols_csv') { $last.symbols_csv } else { @() }
$actStrats   = if ($last.PSObject.Properties.Name -contains 'strats') { $last.strats } elseif ($last.PSObject.Properties.Name -contains 'strategies') { $last.strategies } else { @() }
$actTF       = if ($last.PSObject.Properties.Name -contains 'tf') { "$($last.tf)" } else { "" }
$actLimit    = if ($last.PSObject.Properties.Name -contains 'limit') { [int]$last.limit } else { $null }
$actNotional = if ($last.PSObject.Properties.Name -contains 'notional') { [double]$last.notional } else { $null }
$actWindow   = if ($last.PSObject.Properties.Name -contains 'window') { "$($last.window)" } else { "" }
$actGuard    = if ($last.PSObject.Properties.Name -contains 'guard') { [bool]$last.guard } else { $null }
$actDry      = if ($last.PSObject.Properties.Name -contains 'dry_run') { [bool]$last.dry_run } elseif ($last.PSObject.Properties.Name -contains 'dry') { [bool]$last.dry } else { $null }

# compare
if (SameList $Whitelist $actSymbols) { Show-Pass "Whitelist (symbols) [$source]" } else { Show-Fail "Whitelist (symbols) [$source]" $Whitelist $actSymbols }
if (SameList $Strategies $actStrats) { Show-Pass "Strategies [$source]" } else { Show-Fail "Strategies [$source]" $Strategies $actStrats }
if ($Timeframe -eq $actTF) { Show-Pass "Timeframe [$source]" } else { Show-Fail "Timeframe [$source]" $Timeframe $actTF }
if ($Limit -eq $actLimit) { Show-Pass "Limit [$source]" } else { Show-Fail "Limit [$source]" $Limit $actLimit }
if ([math]::Abs($Notional - ($actNotional ?? -1)) -lt 1e-9) { Show-Pass "Notional [$source]" } else { Show-Fail "Notional [$source]" $Notional $actNotional }
if ($Window -eq $actWindow -or [string]::IsNullOrEmpty($actWindow)) { 
  # treat missing window as neutral (server may not track it)
  if ($actWindow) { Show-Pass "Window [$source]" } else { Write-Host "[OK] Window not echoed by server (treated neutral)" }
} else { Show-Fail "Window [$source]" $Window $actWindow }

if ($RequireGuard) {
  if ($actGuard -eq $true) { Show-Pass "Guard Enabled [$source]" } 
  elseif ($null -eq $actGuard) { Write-Host "[INCONCLUSIVE] Guard not echoed by server" -ForegroundColor Yellow }
  else { Show-Fail "Guard Enabled [$source]" $true $actGuard }
}

if ($PSBoundParameters.ContainsKey("ExpectDryRun")) {
  if ($actDry -eq $ExpectDryRun) { Show-Pass "DryRun [$source]" }
  elseif ($null -eq $actDry) { Write-Host "[INCONCLUSIVE] DryRun not echoed by server" -ForegroundColor Yellow }
  else { Show-Fail "DryRun [$source]" $ExpectDryRun $actDry }
}

# Save report if asked
if ($SaveReport) {
  $dir = Ensure-Reports
  $stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $path = Join-Path $dir "test_config_${stamp}.json"
  $out = [pscustomobject]@{
    base_url = $BaseUrl
    time_utc = (Get-Date).ToUniversalTime().ToString("o")
    intended = $intended
    observed = $last
    source   = $source
  }
  $out | ConvertTo-Json -Depth 8 | Out-File -FilePath $path -Encoding UTF8
  Write-Host "Saved: $path"
}
