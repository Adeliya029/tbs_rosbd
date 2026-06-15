$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  TBS_ROSBD - PRODUCER LAPTOP 1" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Cek Docker containers
$kafkaRunning = docker ps --format "{{.Names}}" | Select-String "rosbd_kafka"
if (-not $kafkaRunning) {
    Write-Host "[!] Kafka container tidak berjalan. Jalankan setup dulu:" -ForegroundColor Red
    Write-Host "    .\deployment\laptop1\setup.ps1" -ForegroundColor Yellow
    exit 1
}

# Source .env
Get-Content "$ProjectRoot\.env" | ForEach-Object {
    if ($_ -match "^\s*([^#][^=]+)=(.+)$") {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim()
        Set-Item -Path "env:$key" -Value $value -ErrorAction SilentlyContinue
    }
}

# Override untuk producer lokal
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

Write-Host "Kafka       : $env:KAFKA_BOOTSTRAP_SERVERS" -ForegroundColor Gray
Write-Host "Topic Gempa : $env:KAFKA_TOPIC_EARTHQUAKE" -ForegroundColor Gray
Write-Host "Topic Logs  : $env:KAFKA_TOPIC_LOGS" -ForegroundColor Gray
Write-Host "Interval    : $env:POLLING_INTERVAL detik" -ForegroundColor Gray
Write-Host "BMKG API    : $env:BMKG_API_URL" -ForegroundColor Gray
Write-Host ""
Write-Host "Tekan Ctrl+C untuk berhenti" -ForegroundColor Yellow
Write-Host ""

# Run producer
Set-Location -Path $ProjectRoot
python producer/realtime_producer.py
