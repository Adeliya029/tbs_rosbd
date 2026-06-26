$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $PSScriptRoot
$ProjectRoot = Split-Path -Parent $ScriptDir
$HistoricalScript = Join-Path $PSScriptRoot "historical_scraper.py"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  TBS_ROSBD - HISTORICAL SCRAPER LAPTOP 1" -ForegroundColor Cyan
Write-Host "  BMKG berita_gempa.php -> MinIO Laptop 2" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Source env dari laptop1
Get-Content "$PSScriptRoot\.env" | ForEach-Object {
    if ($_ -match "^\s*([^#][^=]+)=(.+)$") {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim()
        Set-Item -Path "env:$key" -Value $value -ErrorAction SilentlyContinue
    }
}

# Override untuk historical scraper - pake .env laptop1 dulu, fallback ke hardcoded
if (-not $env:MINIO_ENDPOINT) {
    $env:MINIO_ENDPOINT = "100.122.2.11:9000"
}
if (-not $env:MINIO_ACCESS_KEY) {
    $env:MINIO_ACCESS_KEY = "admin"
}
if (-not $env:MINIO_SECRET_KEY) {
    $env:MINIO_SECRET_KEY = "admin12345"
}
if (-not $env:MINIO_SECURE) {
    $env:MINIO_SECURE = "False"
}

Write-Host "MinIO Endpoint : $env:MINIO_ENDPOINT" -ForegroundColor Gray
Write-Host "MinIO Bucket   : historical-earthquake" -ForegroundColor Gray
Write-Host "BMKG URL       : http://202.90.198.42/gempa/berita_gempa.php" -ForegroundColor Gray
Write-Host "Pages          : 1 - 238" -ForegroundColor Gray
Write-Host ""
Write-Host "Historical scraper akan menjalankan batch scraping" -ForegroundColor Yellow
Write-Host "dan mengirim hasilnya langsung ke MinIO Laptop 2." -ForegroundColor Yellow
Write-Host "Tekan Ctrl+C untuk berhenti" -ForegroundColor Yellow
Write-Host ""

Set-Location -Path $ProjectRoot
python $HistoricalScript
