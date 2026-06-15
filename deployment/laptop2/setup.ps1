$ErrorActionPreference = "Stop"
$Laptop2Dir = $PSScriptRoot
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  TBS_ROSBD - SETUP LAPTOP 2 (CONSUMER)" -ForegroundColor Cyan
Write-Host "  Tailscale IP: 100.122.2.11" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Start Docker containers
Write-Host "[1/4] Start MinIO & Spark containers..." -ForegroundColor Yellow
Set-Location -Path $Laptop2Dir
docker compose down 2>$null
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Gagal start Docker containers" }
Write-Host "  OK" -ForegroundColor Green

# Step 2: Tunggu MinIO siap
Write-Host "[2/4] Waiting for MinIO..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Buat bucket MinIO
$buckets = @(
    "raw-earthquake",
    "historical-earthquake",
    "processed-features",
    "trained-models",
    "spark-checkpoints",
    "analytics"
)

$mcCmd = @"
docker exec rosbd_minio mc alias set local http://localhost:9000 admin admin12345
"@
Invoke-Expression $mcCmd 2>$null

foreach ($bucket in $buckets) {
    $cmd = "docker exec rosbd_minio mc mb local/$bucket --ignore-existing"
    Invoke-Expression $cmd 2>$null
    Write-Host "  Bucket: $bucket" -ForegroundColor Gray
}
Write-Host "  MinIO buckets created!" -ForegroundColor Green

# Step 3: Wait for Spark
Write-Host "[3/4] Waiting for Spark Master..." -ForegroundColor Yellow
Start-Sleep -Seconds 10
Write-Host "  Spark Master: http://localhost:8080" -ForegroundColor Gray
Write-Host "  OK" -ForegroundColor Green

# Step 4: Install Python dependencies
Write-Host "[4/4] Install Python dependencies..." -ForegroundColor Yellow
Set-Location -Path $ProjectRoot
pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Gagal install dependencies" }
Write-Host "  OK" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "  SETUP LAPTOP 2 SELESAI!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Jalankan Spark Streaming job:" -ForegroundColor White
Write-Host "     .\deployment\laptop2\run_streaming.ps1" -ForegroundColor White
Write-Host "  2. Jalankan Spark Batch job (sink ke PostgreSQL):" -ForegroundColor White
Write-Host "     .\deployment\laptop2\run_batch.ps1" -ForegroundColor White
Write-Host "  3. MinIO Console: http://localhost:9001 (admin/admin12345)" -ForegroundColor White
Write-Host "  4. Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host ""

Set-Location -Path $ProjectRoot
