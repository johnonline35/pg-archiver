# pg-archiver
Archive old PostgreSQL data to S3 Parquet files. Perfect for IoT time series data.

## Quick Start
```bash
# Run with Docker
docker run ghcr.io/yourusername/pg-archiver:latest \
  -e PG_CONN_STRING="postgresql://user:pass@host:5432/dbname" \
  -e S3_BUCKET="your-bucket" \
  -e TABLE_NAME="your_iot_table"