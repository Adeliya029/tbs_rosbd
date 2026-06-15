$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  SPARK STREAMING: Kafka Laptop 1 -> MinIO" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Cek containers
$minioRunning = docker ps --format "{{.Names}}" | Select-String "rosbd_minio"
$sparkRunning = docker ps --format "{{.Names}}" | Select-String "rosbd_spark_master"

if (-not $minioRunning) { Write-Host "[!] MinIO tidak jalan. Jalankan setup dulu" -ForegroundColor Red; exit 1 }
if (-not $sparkRunning) { Write-Host "[!] Spark Master tidak jalan. Jalankan setup dulu" -ForegroundColor Red; exit 1 }

Write-Host "Kafka Laptop 1 : 100.76.33.80:9093" -ForegroundColor Gray
Write-Host "MinIO Local    : rosbd_minio:9000" -ForegroundColor Gray
Write-Host ""

# Submit Spark streaming job via spark-submit di container
docker exec `
    -e KAFKA_BOOTSTRAP_SERVERS=100.76.33.80:9093 `
    rosbd_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --name "KafkaToMinIO-Earthquake" `
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" `
    --conf "spark.hadoop.fs.s3a.endpoint=http://rosbd_minio:9000" `
    --conf "spark.hadoop.fs.s3a.access.key=admin" `
    --conf "spark.hadoop.fs.s3a.secret.key=admin12345" `
    --conf "spark.hadoop.fs.s3a.path.style.access=true" `
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" `
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" `
    --conf "spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints/earthquake-stream" `
    /opt/spark/jobs/kafka_to_minio.py

Write-Host ""
Write-Host "Streaming job selesai (atau error)." -ForegroundColor Yellow
