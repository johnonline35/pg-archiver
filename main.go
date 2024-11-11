package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
	"runtime/debug"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	goparquet "github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go-source/local"
)

type IoTRecord struct {
    ID        int64     
    Timestamp time.Time 
    DeviceID  string    
    Value     float64   
    TableName string    // Added to track source table
}

type ParquetFile struct {
    ID        int64  `parquet:"name=id, type=INT64"`
    Timestamp int64  `parquet:"name=timestamp,type=INT64"`
    DeviceID  string `parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8"`
    Value     float64 `parquet:"name=value, type=DOUBLE"`
    TableName string `parquet:"name=table_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func (r IoTRecord) ToParquet() ParquetFile {
    return ParquetFile{
        ID:        r.ID,
        Timestamp: r.Timestamp.UnixNano(),
        DeviceID:  r.DeviceID,
        Value:     r.Value,
        TableName: r.TableName,
    }
}

func writeParquetFile(records []IoTRecord, filename string) error {
    fmt.Printf("Starting to write %d records to parquet file: %s\n", len(records), filename)
    
    fw, err := local.NewLocalFileWriter(filename)
    if err != nil {
        return fmt.Errorf("creating file writer: %w", err)
    }

    pw, err := writer.NewParquetWriter(fw, new(ParquetFile), 1)
    if err != nil {
        fw.Close()
        return fmt.Errorf("creating parquet writer: %w", err)
    }

    pw.CompressionType = goparquet.CompressionCodec_SNAPPY

    for i, record := range records {
        pRecord := record.ToParquet()
        if err := pw.Write(pRecord); err != nil {
            pw.WriteStop()
            fw.Close()
            return fmt.Errorf("writing record %d: %w", i, err)
        }

        if i > 0 && i%50 == 0 {
            fmt.Printf("Wrote %d records\n", i)
        }
    }

    if err := pw.WriteStop(); err != nil {
        fw.Close()
        return fmt.Errorf("stopping writer: %w", err)
    }

    if err := fw.Close(); err != nil {
        return fmt.Errorf("closing file: %w", err)
    }

    return nil
}

func processTable(db *sql.DB, tableName string, cutoff time.Time, batchSize int) ([]IoTRecord, error) {
    query := fmt.Sprintf(`
        SELECT id, timestamp, device_id, value 
        FROM %s 
        WHERE timestamp < $1
        ORDER BY timestamp DESC
        LIMIT $2`, tableName)

    fmt.Printf("Executing query for table %s with cutoff date: %v\n", tableName, cutoff)

    rows, err := db.Query(query, cutoff, batchSize)
    if err != nil {
        return nil, fmt.Errorf("querying old records from %s: %w", tableName, err)
    }
    defer rows.Close()

    var records []IoTRecord
    recordCount := 0

    fmt.Printf("Starting to read records from table: %s\n", tableName)

    for rows.Next() {
        var record IoTRecord
        if err := rows.Scan(&record.ID, &record.Timestamp, &record.DeviceID, &record.Value); err != nil {
            return nil, fmt.Errorf("scanning row from %s: %w", tableName, err)
        }

        record.TableName = tableName
        records = append(records, record)
        recordCount++

        if recordCount%100 == 0 {
            fmt.Printf("Read %d records from %s\n", recordCount, tableName)
        }
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("reading rows from %s: %w", tableName, err)
    }

    fmt.Printf("Finished reading %d records from %s\n", recordCount, tableName)
    return records, nil
}

func deleteArchivedRecords(db *sql.DB, tableName string, cutoff time.Time) (int64, error) {
    result, err := db.Exec(fmt.Sprintf(`
        DELETE FROM %s 
        WHERE timestamp < $1`, tableName), cutoff)
    if err != nil {
        return 0, fmt.Errorf("deleting archived records from %s: %w", tableName, err)
    }

    deleted, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("getting affected rows count from %s: %w", tableName, err)
    }

    return deleted, nil
}

func run() error {
    // Get config from env vars with defaults
    pgConnStr := getEnv("PG_CONN_STRING", "postgresql://localhost:5432/iot?sslmode=disable")
    s3Bucket := getEnv("S3_BUCKET", "my-iot-archive")
    tableNames := getEnv("TABLE_NAMES", "iot_data") // Comma-separated list of tables
    batchSize := 100
    retentionDays := 90

    // Split table names
    tables := strings.Split(tableNames, ",")
    for i := range tables {
        tables[i] = strings.TrimSpace(tables[i])
    }

    fmt.Printf("Starting archival process for tables: %v with batch size: %d\n", tables, batchSize)

    // Connect to Postgres
    db, err := sql.Open("postgres", pgConnStr)
    if err != nil {
        return fmt.Errorf("connecting to postgres: %w", err)
    }
    defer db.Close()

    if err := db.Ping(); err != nil {
        return fmt.Errorf("testing database connection: %w", err)
    }

    fmt.Println("Successfully connected to database")

    // Get data older than retention period
    cutoff := time.Now().AddDate(0, 0, -retentionDays)
    
    // Process each table
    var allRecords []IoTRecord
    for _, tableName := range tables {
        records, err := processTable(db, tableName, cutoff, batchSize)
        if err != nil {
            return fmt.Errorf("processing table %s: %w", tableName, err)
        }
        allRecords = append(allRecords, records...)
    }

    if len(allRecords) == 0 {
        fmt.Println("No records to archive")
        return nil
    }

    // Get latest timestamp for S3 path
    lastTimestamp := allRecords[0].Timestamp
    for _, record := range allRecords[1:] {
        if record.Timestamp.After(lastTimestamp) {
            lastTimestamp = record.Timestamp
        }
    }

    // Write to parquet file
    parquetFile := "/tmp/archive.parquet"
    if err := writeParquetFile(allRecords, parquetFile); err != nil {
        return fmt.Errorf("writing parquet file: %w", err)
    }

    fmt.Println("Successfully wrote parquet file")

    // Upload to S3
    fmt.Println("Configuring AWS client")
    
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        return fmt.Errorf("loading AWS config: %w", err)
    }

    s3Client := s3.NewFromConfig(cfg)
    
    s3Key := fmt.Sprintf("year=%d/month=%02d/multi_table_%s.parquet",
        lastTimestamp.Year(),
        lastTimestamp.Month(),
        lastTimestamp.Format("20060102_150405"))

    fmt.Printf("Uploading to S3: %s/%s\n", s3Bucket, s3Key)

    file, err := os.Open(parquetFile)
    if err != nil {
        return fmt.Errorf("opening parquet file: %w", err)
    }
    defer file.Close()

    _, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
        Bucket: &s3Bucket,
        Key:    &s3Key,
        Body:   file,
    })
    if err != nil {
        return fmt.Errorf("uploading to S3: %w", err)
    }

    fmt.Println("Successfully uploaded to S3")

    // Delete archived records from each table
    for _, tableName := range tables {
        deleted, err := deleteArchivedRecords(db, tableName, cutoff)
        if err != nil {
            return fmt.Errorf("deleting archived records: %w", err)
        }
        fmt.Printf("Deleted %d records from table %s\n", deleted, tableName)
    }

    fmt.Printf("Successfully archived %d total records to s3://%s/%s\n", len(allRecords), s3Bucket, s3Key)

    return nil
}

func main() {
    defer func() {
        if r := recover(); r != nil {
            fmt.Fprintf(os.Stderr, "Panic recovered: %v\nStack trace:\n%s\n", r, debug.Stack())
            os.Exit(1)
        }
    }()

    if err := run(); err != nil {
        fmt.Fprintf(os.Stderr, "error: %v\n", err)
        os.Exit(1)
    }
}

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}