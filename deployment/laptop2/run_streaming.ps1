$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  SPARK STREAMING: Kafka Laptop 1 -> MinIO" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

$minioRunning = docker ps --format "{{.Names}}" | Select-String "rosbd_minio"
$sparkRunning = docker ps --format "{{.Names}}" | Select-String "rosbd_spark_master"

if (-not $minioRunning) { Write-Host "[!] MinIO tidak jalan. Jalankan setup dulu" -ForegroundColor Red; exit 1 }
if (-not $sparkRunning) { Write-Host "[!] Spark Master tidak jalan. Jalankan setup dulu" -ForegroundColor Red; exit 1 }

Write-Host "Kafka Laptop 1 : 100.76.33.80:9093" -ForegroundColor Gray
Write-Host "MinIO Local    : rosbd_minio:9000" -ForegroundColor Gray
Write-Host ""

Write-Host "Menjalankan Spark Streaming job (background)..." -ForegroundColor Yellow
docker exec -u 0 rosbd_spark_master sh -c "mkdir -p /home/spark/.ivy2 /tmp/spark-checkpoints && chown -R spark:spark /home/spark/.ivy2 /tmp/spark-checkpoints" 2>&1 | Out-Null

docker exec -d -e KAFKA_BOOTSTRAP_SERVERS=100.76.33.80:9093 -e MINIO_ENDPOINT=rosbd_minio:9000 rosbd_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --name "KafkaToMinIO-Earthquake" `
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
    --conf "spark.jars.ivy=/home/spark/.ivy2" `
    /opt/spark/jobs/kafka_to_minio.py

Write-Host ""
Write-Host "Spark Streaming BERJALAN!" -ForegroundColor Green
Write-Host "  Cek Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host "  Data output: s3a://raw-earthquake/earthquake-events/ (MinIO)" -ForegroundColor White
Write-Host ""
Write-Host "Untuk stop: docker exec rosbd_spark_master /opt/spark/bin/spark-submit --kill KafkaToMinIO-Earthquake --master spark://spark-master:7077" -ForegroundColor Gray
