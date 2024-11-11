# pg-archiver

Automatically archive time-series data from PostgreSQL to S3 as Parquet files. Perfect for IoT applications where you want to:
- Keep recent data in PostgreSQL for fast querying
- Archive older data to S3 in a cost-effective format
- Maintain data organized by time partitions
- Enable analytical queries on historical data

## Quick Start

```bash
# Using Docker
docker run --rm \
  -e PG_CONN_STRING="postgresql://user:pass@host:5432/dbname" \
  -e S3_BUCKET="my-archive-bucket" \
  -e TABLE_NAMES="iot_data,iot_metrics" \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  ghcr.io/johnonline35/pg-archiver:latest

# Or download a binary from releases:
# https://github.com/johnonline35/pg-archiver/releases
```

## How It Works

1. Connects to your PostgreSQL database
2. Finds data older than retention period (default: 90 days) from specified tables
3. Converts it to Parquet format (optimized for analytical queries)
4. Uploads to S3 with time-based partitioning (year/month)
5. Safely removes archived data from PostgreSQL

## Configuration

Environment variables:
```
PG_CONN_STRING    PostgreSQL connection string (required)
S3_BUCKET         S3 bucket name (required)
AWS_REGION        AWS region (default: us-east-1)
TABLE_NAMES       Comma-separated list of source table names (default: iot_data)
AWS_ENDPOINT_URL  Custom S3 endpoint for testing
```

Expected table schema (for each table):
```sql
CREATE TABLE iot_data (
    id         BIGINT,
    timestamp  TIMESTAMP,
    device_id  TEXT,
    value      DOUBLE PRECISION
);
```

## Local Testing

Test locally using LocalStack:

```bash
# Start LocalStack
docker run -d \
  --name test-s3 \
  -p 4566:4566 \
  -e SERVICES=s3 \
  localstack/localstack:latest

# Create test bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket

# Run archiver with LocalStack
docker run --rm \
  -e PG_CONN_STRING="postgresql://user:pass@host:5432/dbname" \
  -e S3_BUCKET="test-bucket" \
  -e TABLE_NAMES="iot_data,iot_metrics" \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_ENDPOINT_URL="http://localhost:4566" \
  ghcr.io/johnonline35/pg-archiver:latest
```

## Querying Archived Data

The archived Parquet files can be queried using tools like:
- DuckDB
- AWS Athena
- Apache Spark
- Any Parquet-compatible query engine

Example using DuckDB:
```sql
-- Query archived data across all tables
SELECT * 
FROM parquet_scan('s3://my-bucket/year=2024/month=11/*.parquet')
WHERE table_name = 'iot_data' 
  AND device_id = 'sensor1' 
  AND timestamp >= '2024-01-01';

-- Or query specific tables
SELECT * 
FROM parquet_scan('s3://my-bucket/year=2024/month=11/*.parquet')
WHERE table_name IN ('iot_data', 'iot_metrics')
  AND timestamp >= '2024-01-01';
```

## Installation Options

1. Docker:
```bash
docker pull ghcr.io/johnonline35/pg-archiver:latest
```

2. Pre-built binaries:
   - Linux (amd64): `pg-archiver-linux-amd64`
   - macOS (Intel): `pg-archiver-darwin-amd64`
   - macOS (Apple Silicon): `pg-archiver-darwin-arm64`

3. Build from source:
```bash
go install github.com/johnonline35/pg-archiver@latest
```

## Contributing

Contributions welcome! Some areas we'd love help with:
- Real-time archival using NOTIFY/LISTEN
- Support for more column types
- Additional output formats
- Monitoring and metrics
- Custom table schemas support

## License

MIT
