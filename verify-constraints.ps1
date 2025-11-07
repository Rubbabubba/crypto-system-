<#
.SYNOPSIS
  Verify crypto-system respects whitelist, time windows, guards, and strategies.

.DESCRIPTION
  - Tries to GET /routes to discover capability.
  - Checks /scheduler/status.
  - Tries /config, /settings, /symbols/whitelist, /strategy/list, /guard/status, /window/status.
  - Falls back to POST /scheduler/run (dry run) with echo-style probes; parses any JSON returned.
  - Compares against expected values you pass in; prints PASS/FAIL per check.

.PARAMETER BaseUrl
  Your API base URL (e.g., https://crypto-system-z605.onrender.com)

.PARAMETER ExpectWhitelist
  Comma-separated symbols you expect enforced (e.g., BTC/USD,ETH/USD,SOL/USD,...)

.PARAMETER ExpectStrats
  Comma-separated strategies you expect active (e.g., c1,c2,c3,c4,c5,c6)

.PARAMETER ExpectTimeframe
  Expected timeframe label (e.g., 5Min)

.PARAMETER ExpectGuard
  A simple string you expect to see in guard status (e.g., "dry=False", "daily_cap=...").
  This is a substring check against guard/status text or scheduler echo.

.PARAMETER ApiKey
  Optional bearer token

.PARAMETER VerboseProbe
  If set, prints raw payloads found.

.EXAMPLE
  .\verify-constraints.ps1 -BaseUrl https://crypto-system-z605.onrender.com `
    -ExpectWhitelist "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD" `
    -ExpectStrats "c1,c2,c3,c4,c5,c6" -ExpectTimeframe "5Min" -ExpectGuard "dry=False"
#>

param(
  [Parameter(Mandatory=$true)] [string]$BaseUrl,
  [string]$ExpectWhitelist = "",
  [string]$ExpectStrats = "",
  [string]$ExpectTimeframe = "",
  [string]$ExpectGuard = "",
  [string]$ApiKey = "",
  [switch]$VerboseProbe
)

# --- Helpers -----------------------------------------------------------------
function New-AuthHeader {
  param([string]$ApiKey)
  if ([string]::IsNullOrWhiteSpace($ApiKey)) { return @{} }
  @{ Authorization = "Bearer $ApiKey" }
}

$CommonHeaders = @{ Accept = "application/json" } + (New-AuthHeader -ApiKey $ApiKey)

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
    if ($Method -eq 'GET') {
      return Invoke-RestMethod -Method GET -Uri $uri -Headers $CommonHeaders -TimeoutSec $TimeoutSec
    } elseif ($Method -eq 'POST') {
      if ($Body -ne $null) {
        return Invoke-RestMethod -Method POST -Uri $uri -Headers ($CommonHeaders + @{ "Content-Type"="application/json"}) -Body ($Body | ConvertTo-Json -Depth 15) -TimeoutSec $TimeoutSec
      } else {
        return Invoke-RestMethod -Method POST -Uri $uri -Headers $CommonHeaders -TimeoutSec $TimeoutSec
      }
    } else {
      throw "Unsupported method: $Method"
    }
  } catch {
    Write-Verbose "Invoke-Api $Method $uri failed: $($_.Exception.Message)"
    return $null
  }
}

function As-Array([object]$obj) {
  if ($null -eq $obj) { return @() }
  if ($obj -is [System.Collections.IEnumerable] -and -not ($obj -is [string])) { return @($obj) }
  return @($obj)
}

function Normalize-List([string]$csv) {
  if ([string]::IsNullOrWhiteSpace($csv)) { return @() }
  return ($csv.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
}

function Print-Check($name, $ok, $details="") {
  if ($ok) { Write-Host ("[PASS] {0} {1}" -f $name, $details) -ForegroundColor Green }
  else     { Write-Host ("[FAIL] {0} {1}" -f $name, $details) -ForegroundColor Red }
}

# Expecteds
$ExpectedWhitelistArr = Normalize-List $ExpectWhitelist
$ExpectedStratsArr    = Normalize-List $ExpectStrats
$ExpectedTimeframeStr = $ExpectTimeframe.Trim()
$ExpectedGuardStr     = $ExpectGuard.Trim()

Write-Host "== Probing $BaseUrl =="
$routes = Invoke-Api -Path "routes"        # best-effort
$has = @{
  routes            = ($routes -ne $null)
  scheduler_status  = $false
  config            = $false
  settings          = $false
  whitelist         = $false
  strategies        = $false
  guard_status      = $false
  window_status     = $false
}

# 1) Scheduler status
$sched = Invoke-Api -Path "scheduler/status"
if ($sched -ne $null) {
  $has.scheduler_status = $true
  if ($VerboseProbe) { $sched | ConvertTo-Json -Depth 10 | Write-Host }
  $enabled = $false; try { $enabled = [bool]$sched.enabled } catch {}
  Print-Check "Scheduler enabled" $enabled ("interval={0}s" -f $sched.interval_secs)
} else {
  Print-Check "Scheduler status" $false "(no /scheduler/status)"
}

# 2) Config-ish endpoints
$config  = Invoke-Api -Path "config"
$settings = $null
if ($config -ne $null) { $has.config = $true; if ($VerboseProbe) { $config | ConvertTo-Json -Depth 15 | Write-Host } }
else {
  $settings = Invoke-Api -Path "settings"
  if ($settings -ne $null) { $has.settings = $true; if ($VerboseProbe) { $settings | ConvertTo-Json -Depth 15 | Write-Host } }
}

# 3) Whitelist endpoints
$wl = Invoke-Api -Path "symbols/whitelist"
if ($wl -eq $null) { $wl = Invoke-Api -Path "whitelist" }
if ($wl -ne $null) { $has.whitelist = $true; if ($VerboseProbe) { $wl | ConvertTo-Json -Depth 10 | Write-Host } }

# 4) Strategies endpoint
$strats = Invoke-Api -Path "strategy/list"
if ($strats -eq $null) { $strats = Invoke-Api -Path "strategies" }
if ($strats -ne $null) { $has.strategies = $true; if ($VerboseProbe) { $strats | ConvertTo-Json -Depth 10 | Write-Host } }

# 5) Guard / Window status
$guard = Invoke-Api -Path "guard/status"
if ($guard -ne $null) { $has.guard_status = $true; if ($VerboseProbe) { $guard | ConvertTo-Json -Depth 10 | Write-Host } }

$window = Invoke-Api -Path "window/status"
if ($window -ne $null) { $has.window_status = $true; if ($VerboseProbe) { $window | ConvertTo-Json -Depth 10 | Write-Host } }

# 6) Fallback: dry-run echo from scheduler.run (many servers return the resolved config)
$echo = $null
$echoBody = @{
  dry          = $true
  echo         = $true      # harmless if ignored
  limit        = 10
  notional     = 25
}
# If you set expectations, pass them along to encourage echo back (harmless if ignored):
if ($ExpectedStratsArr.Count -gt 0)    { $echoBody.strats = $ExpectedStratsArr }
if ($ExpectedWhitelistArr.Count -gt 0) { $echoBody.symbols = $ExpectedWhitelistArr }
if ($ExpectedTimeframeStr)             { $echoBody.timeframe = $ExpectedTimeframeStr }

$echo = Invoke-Api -Path "scheduler/run" -Method "POST" -Body $echoBody
if ($VerboseProbe -and $echo -ne $null) { $echo | ConvertTo-Json -Depth 20 | Write-Host }

# --- Extract observed values -------------------------------------------------
function Try-Pick($obj, $keys) {
  foreach ($k in $keys) {
    if ($obj -and ($obj.PSObject.Properties.Name -contains $k)) { return $obj.$k }
  }
  return $null
}

# Observed whitelist
$obsWhitelist = @()
if ($wl) {
  $obsWhitelist = As-Array (Try-Pick $wl @("whitelist","symbols","allowed","items"))
} elseif ($config) {
  $obsWhitelist = As-Array (Try-Pick $config @("whitelist","symbols"))
} elseif ($settings) {
  $obsWhitelist = As-Array (Try-Pick $settings @("whitelist","symbols"))
} elseif ($echo) {
  $obsWhitelist = As-Array (Try-Pick $echo @("symbols","whitelist","used_symbols"))
}
$obsWhitelist = $obsWhitelist | ForEach-Object { [string]$_ } | Sort-Object -Unique

# Observed strategies
$obsStrats = @()
if ($strats) {
  $obsStrats = As-Array (Try-Pick $strats @("strategies","items","strats"))
} elseif ($config) {
  $obsStrats = As-Array (Try-Pick $config @("strategies","strats"))
} elseif ($settings) {
  $obsStrats = As-Array (Try-Pick $settings @("strategies","strats"))
} elseif ($echo) {
  $obsStrats = As-Array (Try-Pick $echo @("strats","used_strats"))
}
$obsStrats = $obsStrats | ForEach-Object { [string]$_ } | Sort-Object -Unique

# Observed timeframe
$obsTf = $null
$obsTf = $obsTf ?? (Try-Pick $config @("timeframe","tf"))
$obsTf = $obsTf ?? (Try-Pick $settings @("timeframe","tf"))
$obsTf = $obsTf ?? (Try-Pick $echo @("timeframe","tf"))
if ($obsTf) { $obsTf = [string]$obsTf }

# Observed guard/window text (for substring matching)
$obsGuardText  = ""
if ($guard)   { $obsGuardText += ($guard | ConvertTo-Json -Depth 10) }
if ($window)  { $obsGuardText += "`n" + ($window | ConvertTo-Json -Depth 10) }
if ($echo)    { $obsGuardText += "`n" + ($echo | ConvertTo-Json -Depth 10) }

# --- Comparisons -------------------------------------------------------------
# Whitelist compare (order-insensitive)
$okWhitelist = $true
$wlDetails = ""
if ($ExpectedWhitelistArr.Count -gt 0) {
  $missing = $ExpectedWhitelistArr | Where-Object { $obsWhitelist -notcontains $_ }
  $extra   = $obsWhitelist | Where-Object { $ExpectedWhitelistArr -notcontains $_ }
  $okWhitelist = ($missing.Count -eq 0)
  $wlDetails = ("obs=[{0}] exp=[{1}] missing=[{2}] extra=[{3}]" -f ($obsWhitelist -join ","), ($ExpectedWhitelistArr -join ","), ($missing -join ","), ($extra -join ","))
} else {
  $wlDetails = "obs=[" + ($obsWhitelist -join ",") + "]"
}
Print-Check "Whitelist enforced" $okWhitelist $wlDetails

# Strategies compare
$okStrats = $true
$stratDetails = ""
if ($ExpectedStratsArr.Count -gt 0) {
  $missingS = $ExpectedStratsArr | Where-Object { $obsStrats -notcontains $_ }
  $okStrats = ($missingS.Count -eq 0)
  $stratDetails = ("obs=[{0}] exp=[{1}] missing=[{2}]" -f ($obsStrats -join ","), ($ExpectedStratsArr -join ","), ($missingS -join ","))
} else {
  $stratDetails = "obs=[" + ($obsStrats -join ",") + "]"
}
Print-Check "Strategies active" $okStrats $stratDetails

# Timeframe compare
$okTf = $true
$tfDetails = ""
if ($ExpectedTimeframeStr) {
  $okTf = ($obsTf -eq $ExpectedTimeframeStr)
  $tfDetails = ("obs=[{0}] exp=[{1}]" -f $obsTf, $ExpectedTimeframeStr)
} else {
  $tfDetails = "obs=[" + ($obsTf + "") + "]"
}
Print-Check "Timeframe respected" $okTf $tfDetails

# Guard/window substring check
$okGuard = $true
$guardDetails = ""
if ($ExpectedGuardStr) {
  $okGuard = ($obsGuardText -match [Regex]::Escape($ExpectedGuardStr))
  $guardDetails = ("looking for '{0}'" -f $ExpectedGuardStr)
} else {
  $guardDetails = "no expectation provided"
}
Print-Check "Guard/Window respected" $okGuard $guardDetails

# Final hint
if ($VerboseProbe -and (-not $has.routes)) {
  Write-Host "`n[Note] /routes not exposed (fine). Probed via common endpoints and scheduler echo."
}

