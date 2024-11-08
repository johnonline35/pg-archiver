package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
}

// ParquetFile represents our parquet schema
type ParquetFile struct {
    ID        int64  `parquet:"name=id, type=INT64"`
    Timestamp int64  `parquet:"name=timestamp,type=INT64"`
    DeviceID  string `parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8"`
    Value     float64 `parquet:"name=value, type=DOUBLE"`
}

func (r IoTRecord) ToParquet() ParquetFile {
    return ParquetFile{
        ID:        r.ID,
        Timestamp: r.Timestamp.UnixNano(),
        DeviceID:  r.DeviceID,
        Value:     r.Value,
    }
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

func writeParquetFile(records []IoTRecord, filename string) error {
    fmt.Printf("Starting to write %d records to parquet file: %s\n", len(records), filename)
    
    // Create new ParquetFile
    fw, err := local.NewLocalFileWriter(filename)
    if err != nil {
        return fmt.Errorf("creating file writer: %w", err)
    }

    // Configure writer
    pw, err := writer.NewParquetWriter(fw, new(ParquetFile), 1)
    if err != nil {
        fw.Close()
        return fmt.Errorf("creating parquet writer: %w", err)
    }

    // Set basic configuration
    pw.CompressionType = goparquet.CompressionCodec_SNAPPY

    // Write records
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

    // Close writer
    if err := pw.WriteStop(); err != nil {
        fw.Close()
        return fmt.Errorf("stopping writer: %w", err)
    }

    // Close file
    if err := fw.Close(); err != nil {
        return fmt.Errorf("closing file: %w", err)
    }

    return nil
}

func run() error {
	// Get config from env vars with defaults
	pgConnStr := getEnv("PG_CONN_STRING", "postgresql://localhost:5432/iot?sslmode=disable")
	s3Bucket := getEnv("S3_BUCKET", "my-iot-archive")
	tableName := getEnv("TABLE_NAME", "iot_data")
	batchSize := 100
	retentionDays := 90

	fmt.Printf("Starting archival process with batch size: %d\n", batchSize)

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
	query := fmt.Sprintf(`
		SELECT id, timestamp, device_id, value 
		FROM %s 
		WHERE timestamp < $1
		ORDER BY timestamp DESC
		LIMIT $2`, tableName)

	fmt.Printf("Executing query with cutoff date: %v\n", cutoff)

	rows, err := db.Query(query, cutoff, batchSize)
	if err != nil {
		return fmt.Errorf("querying old records: %w", err)
	}
	defer rows.Close()

    var records []IoTRecord
    var lastTimestamp time.Time
    recordCount := 0

    fmt.Println("Starting to read records from database")

    for rows.Next() {
        var record IoTRecord
        if err := rows.Scan(&record.ID, &record.Timestamp, &record.DeviceID, &record.Value); err != nil {
            return fmt.Errorf("scanning row: %w", err)
        }

        if recordCount == 0 {
            lastTimestamp = record.Timestamp
        }

        records = append(records, record)
        recordCount++

        if recordCount%100 == 0 {
            fmt.Printf("Read %d records from database\n", recordCount)
        }
    }

    if err := rows.Err(); err != nil {
        return fmt.Errorf("reading rows: %w", err)
    }

    fmt.Printf("Finished reading %d records from database\n", recordCount)

    if recordCount == 0 {
        fmt.Println("No records to archive")
        return nil
    }

    // Write to parquet file
    parquetFile := "/tmp/archive.parquet"
    if err := writeParquetFile(records, parquetFile); err != nil {
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
	
	s3Key := fmt.Sprintf("year=%d/month=%02d/%s_%s.parquet",
		lastTimestamp.Year(),
		lastTimestamp.Month(),
		tableName,
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

	// Delete archived records
	fmt.Println("Deleting archived records from database")
	
	result, err := db.Exec(fmt.Sprintf(`
		DELETE FROM %s 
		WHERE timestamp < $1`, tableName), cutoff)
	if err != nil {
		return fmt.Errorf("deleting archived records: %w", err)
	}

	deleted, _ := result.RowsAffected()
	fmt.Printf("Successfully archived %d records to s3://%s/%s\n", recordCount, s3Bucket, s3Key)
	fmt.Printf("Deleted %d records from postgres\n", deleted)

	return nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}