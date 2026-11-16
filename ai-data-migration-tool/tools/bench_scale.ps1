# Measures time and peak memory of the original in-memory engine vs the streaming engine.
#   powershell -ExecutionPolicy Bypass -File tools\bench_scale.ps1 -Rows "200000,1000000" [-SkipOldAbove 300000]
param([string]$Rows = "200000,1000000", [int]$SkipOldAbove = 300000, [string]$Work = "$env:TEMP\amt_bench")

$ErrorActionPreference = "Stop"
$RowList = @($Rows -split "," | ForEach-Object { [int]$_.Trim() })
New-Item -ItemType Directory -Force $Work | Out-Null
$py = (Get-Command python).Source
$here = Split-Path -Parent $PSScriptRoot

function Measure-Run([string]$label, [string[]]$argv) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $p = Start-Process -FilePath $py -ArgumentList $argv -WorkingDirectory $here -PassThru -NoNewWindow `
        -RedirectStandardOutput "$Work\out.txt" -RedirectStandardError "$Work\err.txt"
    $peak = 0
    while (-not $p.HasExited) {
        try { $p.Refresh(); if ($p.PeakWorkingSet64 -gt $peak) { $peak = $p.PeakWorkingSet64 } } catch {}
        Start-Sleep -Milliseconds 200
    }
    $sw.Stop()
    $status = if (Select-String -Path "$Work\out.txt", "$Work\err.txt" -Pattern "FAIL|ERROR|Traceback" -Quiet) { "FAIL/ERROR" } else { "PASS" }
    [pscustomobject]@{ Run = $label; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1); PeakMemoryMB = [math]::Round($peak / 1MB); Result = $status }
}

$results = @()
foreach ($n in $RowList) {
    $db = "$Work\orders_$n.db"
    if (-not (Test-Path $db)) { & $py "$here\tools\make_big_table.py" $db --rows $n --customers 20000 | Out-Null }
    $dbu = $db -replace '\\', '/'
    @"
name: bench-$n
source: sqlite:///$dbu
target: $(($Work -replace '\\','/'))/lake_stream_$n
format: parquet
crosscheck: true
state_file: $(($Work -replace '\\','/'))/state_$n.json
report_dir: $(($Work -replace '\\','/'))/reports_$n
tables:
  - name: orders
    partition_by: updated_at
"@ | Out-File -Encoding utf8 "$Work\bench_$n.yaml"
    $results += Measure-Run "streaming engine, $n rows" @("-m", "amt", "run", "$Work\bench_$n.yaml", "--full")
    if ($n -le $SkipOldAbove) {
        $results += Measure-Run "original engine, $n rows" @("-m", "amt", "migrate", "--source", "sqlite:///$dbu", "--table", "orders",
            "--target", "$Work\lake_old_$n", "--partition-by", "updated_at", "--report-dir", "$Work\old_reports_$n")
    }
}
$results | Format-Table -AutoSize
