<#
End-to-end test for Crypto System API.
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)]
  [string]$BaseUrl,
  [int]$SinceHours = 48,
  [string]$StrategyToAttach = "",
  [int]$PriceCheckLimit = 6
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if ($BaseUrl.EndsWith('/')) { $BaseUrl = $BaseUrl.TrimEnd('/') }

function Write-Info($msg)  { Write-Host "[INFO ] $msg"  -ForegroundColor Cyan }
function Write-OK($msg)    { Write-Host "[ OK  ] $msg"  -ForegroundColor Green }
function Write-Warn($msg)  { Write-Host "[WARN ] $msg"  -ForegroundColor Yellow }
function Write-Err($msg)   { Write-Host "[FAIL] $msg"   -ForegroundColor Red }

function Invoke-Json {
  param(
    [Parameter(Mandatory=$true)][ValidateSet('GET','POST')]
    [string]$Method,
    [Parameter(Mandatory=$true)]
    [string]$Path,
    [hashtable]$Body = $null,
    [int]$TimeoutSec = 30
  )
  $url = "$BaseUrl$Path"
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  try {
    if ($Method -eq 'GET') {
      $resp = Invoke-RestMethod -Method GET -Uri $url -TimeoutSec $TimeoutSec
    } else {
      $json = if ($Body) { $Body | ConvertTo-Json -Depth 10 } else { $null }
      $resp = Invoke-RestMethod -Method POST -Uri $url -Body $json -ContentType 'application/json' -TimeoutSec $TimeoutSec
    }
    $sw.Stop()
    return @{ ok=$true; ms=$sw.ElapsedMilliseconds; data=$resp; url=$url }
  } catch {
    $sw.Stop()
    return @{ ok=$false; ms=$sw.ElapsedMilliseconds; error="$($_.Exception.Message)"; url=$url }
  }
}

function Show-Table {
  param(
    [Parameter(Mandatory=$true)]
    [Array]$Rows,
    [string[]]$Select = @()
  )
  if (-not $Rows -or $Rows.Count -eq 0) { Write-Warn "No rows."; return }
  if ($Select.Count -gt 0) {
    $Rows | Select-Object -Property $Select | Format-Table -AutoSize | Out-Host
  } else {
    $Rows | Format-Table -AutoSize | Out-Host
  }
}

$report = [ordered]@{
  startedUtc = (Get-Date).ToUniversalTime().ToString("s") + "Z"
  baseUrl    = $BaseUrl
  steps      = @{}
}

# 1) Health
Write-Info "Checking /health ..."
$R_health = Invoke-Json GET "/health"
$report.steps.health = $R_health
if ($R_health.ok -and $R_health.data.ok) {
  Write-OK ("/health ok ({0} ms)" -f $R_health.ms)
} else {
  Write-Err "/health failed: $($R_health.error)"; exit 1
}

# 2) Config
Write-Info "Fetching /config ..."
$R_config = Invoke-Json GET "/config"
$report.steps.config = $R_config
if (-not $R_config.ok -or -not $R_config.data.ok) { Write-Err "/config failed: $($R_config.error)"; exit 1 }
$cfg = $R_config.data
Write-OK ("Service: {0} v{1}; TZ={2}; trading_enabled={3}" -f $cfg.service, $cfg.version, $cfg.tz, $cfg.trading_enabled)
$symbols    = if ($cfg.symbols) { @($cfg.symbols) } else { @() }
$strategies = if ($cfg.strategies) { @($cfg.strategies) } else { @() }
if ($symbols.Count -eq 0)   { Write-Warn "No symbols returned by /config." }
if ($strategies.Count -eq 0){ Write-Warn "No strategies returned by /config." }

# 3) Price pings
$testSymbols = $symbols | Select-Object -First $PriceCheckLimit
$prices = @{}
foreach ($sym in $testSymbols) {
  if ($sym -notmatch '^[A-Z0-9]+/[A-Z0-9]+$') { Write-Warn "Skipping malformed symbol '${sym}'"; continue }
  $base,$quote = $sym.Split('/')
  $R_px = Invoke-Json GET "/price/$base/$quote"
  if ($R_px.ok -and $R_px.data.ok) {
    $prices[$sym] = [double]$R_px.data.price
    Write-OK ("Price {0,-10} = {1}" -f $sym, $R_px.data.price)
  } else {
    Write-Warn "Price failed for ${sym}: $($R_px.error)"
  }
}
$report.steps.prices = $prices

# 4) Journal sync
Write-Info ("POST /journal/sync since_hours={0} (limit=50000) ..." -f $SinceHours)
$R_sync = Invoke-Json POST "/journal/sync" @{ since_hours = $SinceHours; limit = 50000 }
$report.steps.journalSync = $R_sync
if ($R_sync.ok -and $R_sync.data.ok) {
  Write-OK ("journal/sync ok. updated={0} seen={1}" -f $R_sync.data.updated, $R_sync.data.count)
} else {
  Write-Warn "journal/sync failed: $($R_sync.error)"
}

# 5) Journal peek + optional attach
Write-Info "GET /journal?limit=50 ..."
$R_journal = Invoke-Json GET "/journal?limit=50"
$report.steps.journalPeek = $R_journal
if ($R_journal.ok -and $R_journal.data.ok) {
  $rows = @($R_journal.data.rows)

  # --- Normalize: ensure every row has a 'strategy' property (might be missing on older records)
  $rows = $rows | ForEach-Object {
    if (-not $_.PSObject.Properties['strategy']) {
      $_ | Add-Member -NotePropertyName strategy -NotePropertyValue $null -PassThru
    } else { $_ }
  }

  Write-OK ("Journal rows returned: {0} (total: {1})" -f $rows.Count, $R_journal.data.count)
  if ($rows.Count -gt 0) {
    $rows | Select-Object txid, ts, symbol, side, price, volume, fee, strategy | Format-Table -AutoSize | Out-Host
  } else {
    Write-Warn "Journal is empty."
  }

  if ($StrategyToAttach) {
    # --- Safe check: treat missing OR empty strategy as unlabeled
    $firstUnlabeled = $rows | Where-Object {
      $p = $_.PSObject.Properties['strategy']
      (-not $p) -or (-not $p.Value)
    } | Select-Object -First 1

    if ($null -ne $firstUnlabeled) {
      $tx = $firstUnlabeled.txid
      Write-Info ("Attaching strategy '{0}' to txid={1} ..." -f $StrategyToAttach, $tx)
      $R_attach = Invoke-Json POST "/journal/attach" @{ txid = $tx; strategy = $StrategyToAttach }
      $report.steps.journalAttach = $R_attach
      if ($R_attach.ok -and $R_attach.data.ok -and $R_attach.data.updated -ge 1) {
        Write-OK "Attached strategy."
      } else {
        Write-Warn "Attach failed or no rows updated: $($R_attach.error)"
      }
    } else {
      Write-Warn "No unlabeled journal rows available to attach."
    }
  }
} else {
  Write-Err "Failed to read journal: $($R_journal.error)"
}

# 6) Fills
Write-Info "GET /fills?limit=25 ..."
$R_fills = Invoke-Json GET "/fills?limit=25"
$report.steps.fills = $R_fills
if ($R_fills.ok -and $R_fills.data.ok) {
  Write-OK ("Fills returned: {0}" -f $R_fills.data.count)
  $fillRows = @($R_fills.data.rows)
  if ($fillRows.Count -gt 0) {
    $fillRows | Select-Object ts, symbol, side, price, volume, fee, strategy | Format-Table -AutoSize | Out-Host
  }
} else {
  Write-Warn "fills failed: $($R_fills.error)"
}

# 7) P&L summary
Write-Info "GET /pnl/summary ..."
$R_pnl = Invoke-Json GET "/pnl/summary"
$report.steps.pnlSummary = $R_pnl
if ($R_pnl.ok -and $R_pnl.data) {
  $pnl = $R_pnl.data
  $total = $pnl.total
  if ($total) {
    Write-OK ("TOTAL  realized={0:N2}  unrealized={1:N2}  fees={2:N2}  equity={3:N2}" -f $total.realized,$total.unrealized,$total.fees,$total.equity)
  } else {
    Write-Warn "No total P&L returned."
  }

  $byStrat = @($pnl.per_strategy)
  if ($byStrat.Count -gt 0) {
    Write-Info "P&L by Strategy:"
    Show-Table -Rows $byStrat -Select @('strategy','realized','unrealized','fees','equity')
  } else {
    Write-Warn "No per_strategy breakdown."
  }

  $bySym = @($pnl.per_symbol)
  if ($bySym.Count -gt 0) {
    Write-Info "P&L by Symbol:"
    Show-Table -Rows $bySym -Select @('symbol','realized','unrealized','fees','equity')
  } else {
    Write-Warn "No per_symbol breakdown."
  }
} else {
  Write-Err "Failed to get P&L: $($R_pnl.error)"
}

# 8) Advisor daily/apply roundtrip
Write-Info "GET /advisor/daily ..."
$R_adv0 = Invoke-Json GET "/advisor/daily"
$report.steps.advisorDailyBefore = $R_adv0
if (-not $R_adv0.ok) { Write-Warn "advisor/daily failed: $($R_adv0.error)" }

$sampleApply = @{
  date = (Get-Date).ToString("yyyy-MM-dd")
  notes = "E2E test apply at $(Get-Date -Format s)"
  recommendations = @(
    @{ strategy="c1"; action="reduce"; reason="risk cap test"; symbol="BTC/USD" },
    @{ strategy="c2"; action="increase"; reason="momentum test"; symbol="ETH/USD" }
  )
}
Write-Info "POST /advisor/apply ..."
$R_advApply = Invoke-Json POST "/advisor/apply" $sampleApply
$report.steps.advisorApply = $R_advApply
if ($R_advApply.ok -and $R_advApply.data.ok) {
  Write-OK "advisor/apply saved."
} else {
  Write-Warn "advisor/apply failed: $($R_advApply.error)"
}

Write-Info "GET /advisor/daily (verify) ..."
$R_adv1 = Invoke-Json GET "/advisor/daily"
$report.steps.advisorDailyAfter = $R_adv1
if ($R_adv1.ok -and $R_adv1.data.ok) {
  $after = $R_adv1.data
  Write-OK ("advisor/daily ok. date={0}; rec_count={1}" -f $after.date, @($after.recommendations).Count)
} else {
  Write-Warn "advisor/daily verification failed: $($R_adv1.error)"
}

# 9) Scheduler toggles
Write-Info "GET /scheduler/status ..."
$R_ss = Invoke-Json GET "/scheduler/status"
$report.steps.schedulerStatus0 = $R_ss
if ($R_ss.ok) { Write-OK ("scheduler status: enabled={0} interval={1}s" -f $R_ss.data.enabled, $R_ss.data.interval_secs) }

Write-Info "POST /scheduler/start ..."
$R_sStart = Invoke-Json POST "/scheduler/start" @{}
$report.steps.schedulerStart = $R_sStart
if ($R_sStart.ok) { Write-OK "scheduler started." } else { Write-Warn "start failed: $($R_sStart.error)" }

Write-Info "GET /scheduler/status ..."
$R_ss1 = Invoke-Json GET "/scheduler/status"
$report.steps.schedulerStatus1 = $R_ss1
if ($R_ss1.ok) { Write-OK ("scheduler status: enabled={0}" -f $R_ss1.data.enabled) }

Write-Info "POST /scheduler/stop ..."
$R_sStop = Invoke-Json POST "/scheduler/stop" @{}
$report.steps.schedulerStop = $R_sStop
if ($R_sStop.ok) { Write-OK "scheduler stopped." } else { Write-Warn "stop failed: $($R_sStop.error)" }

Write-Info "GET /scheduler/status ..."
$R_ss2 = Invoke-Json GET "/scheduler/status"
$report.steps.schedulerStatus2 = $R_ss2
if ($R_ss2.ok) { Write-OK ("scheduler status: enabled={0}" -f $R_ss2.data.enabled) }

# 10) Save report
$report.finishedUtc = (Get-Date).ToUniversalTime().ToString("s") + "Z"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outPath = Join-Path (Get-Location) "crypto-system-test-REPORT-$stamp.json"
$report | ConvertTo-Json -Depth 12 | Out-File -FilePath $outPath -Encoding UTF8
Write-OK "Wrote report: $outPath"
