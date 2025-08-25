package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
)

func main() {
	fmt.Println("🔍 Validating CDC content in parquet files...")

	files := []string{
		"/tmp/pg_flo_direct_parquet/public.users_20250824_164018.parquet",
		"/tmp/pg_flo_direct_parquet/public.transactions_20250824_164013.parquet", // Has 5 rows
	}

	totalRecords := 0
	for _, filePath := range files {
		fmt.Printf("\n📋 Analyzing: %s\n", filePath)
		records, err := validateCDCFile(filePath)
		if err != nil {
			log.Printf("❌ Failed to validate %s: %v", filePath, err)
			continue
		}
		totalRecords += records
	}

	fmt.Printf("\n🎯 Summary: Successfully validated %d CDC records with proper operations, LSNs, and metadata\n", totalRecords)
}

func validateCDCFile(filePath string) (int, error) {
	// Open parquet file
	fileReader, err := file.OpenParquetFile(filePath, false)
	if err != nil {
		return 0, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer fileReader.Close()

	reader, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return 0, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	schema, err := reader.Schema()
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	fmt.Printf("  📊 Schema fields: %d\n", schema.NumFields())

	// Validate required CDC fields exist
	requiredFields := []string{"_pg_flo_operation", "_pg_flo_lsn", "_pg_flo_timestamp", "_pg_flo_schema", "_pg_flo_table"}
	for _, field := range requiredFields {
		if schema.FieldIndices(field) == nil {
			return 0, fmt.Errorf("missing required CDC field: %s", field)
		}
	}
	fmt.Printf("  ✅ All required CDC metadata fields present\n")

	// Read and validate data
	recordReader, err := reader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get record reader: %w", err)
	}
	defer recordReader.Release()

	totalRecords := 0
	operationCounts := make(map[string]int)
	lsnCount := 0
	timestampCount := 0

	for recordReader.Next() {
		record := recordReader.Record()
		numRows := int(record.NumRows())
		totalRecords += numRows

		// Get column indices
		opIdx := schema.FieldIndices("_pg_flo_operation")
		lsnIdx := schema.FieldIndices("_pg_flo_lsn")
		tsIdx := schema.FieldIndices("_pg_flo_timestamp")

		if len(opIdx) > 0 {
			opCol := record.Column(opIdx[0])
			for i := 0; i < numRows; i++ {
				if !opCol.IsNull(i) {
					// Extract operation value
					if strArray, ok := opCol.(*array.String); ok {
						op := strArray.Value(i)
						operationCounts[op]++
					}
				}
			}
		}

		if len(lsnIdx) > 0 {
			lsnCol := record.Column(lsnIdx[0])
			for i := 0; i < numRows; i++ {
				if !lsnCol.IsNull(i) {
					lsnCount++
				}
			}
		}

		if len(tsIdx) > 0 {
			tsCol := record.Column(tsIdx[0])
			for i := 0; i < numRows; i++ {
				if !tsCol.IsNull(i) {
					timestampCount++
				}
			}
		}

		record.Release()
	}

	fmt.Printf("  📊 Total records: %d\n", totalRecords)
	fmt.Printf("  📈 Operations breakdown:\n")
	for op, count := range operationCounts {
		fmt.Printf("    - %s: %d\n", op, count)
	}
	fmt.Printf("  📍 LSN values: %d/%d records\n", lsnCount, totalRecords)
	fmt.Printf("  ⏰ Timestamp values: %d/%d records\n", timestampCount, totalRecords)

	// Validation checks
	if totalRecords == 0 {
		return 0, fmt.Errorf("no records found in file")
	}

	if len(operationCounts) == 0 {
		return 0, fmt.Errorf("no operation types found")
	}

	// Check that we have expected operation types
	validOps := []string{"INSERT", "UPDATE", "DELETE"}
	hasValidOp := false
	for op := range operationCounts {
		for _, validOp := range validOps {
			if strings.Contains(op, validOp) {
				hasValidOp = true
				break
			}
		}
	}

	if !hasValidOp {
		return 0, fmt.Errorf("no valid CDC operations found: %v", operationCounts)
	}

	if lsnCount != totalRecords {
		return 0, fmt.Errorf("LSN coverage incomplete: %d/%d", lsnCount, totalRecords)
	}

	if timestampCount != totalRecords {
		return 0, fmt.Errorf("timestamp coverage incomplete: %d/%d", timestampCount, totalRecords)
	}

	fmt.Printf("  ✅ CDC validation passed\n")
	return totalRecords, nil
}
