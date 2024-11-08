package load

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
)

func AddTickerColumn(csvData []byte, ticker string) ([]byte, error) {
	// Create a reader for the CSV data
	reader := csv.NewReader(bytes.NewReader(csvData))

	// Create a buffer to hold the modified CSV data
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Append the "ticker" column name to the header
	header = append(header, "ticker")

	// Write the modified header to the buffer
	if err := writer.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Read and modify the remaining CSV data
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV data: %w", err)
		}

		// Append the ticker value to the record
		record = append(record, ticker)

		// Write the modified record to the buffer
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV data: %w", err)
		}
	}

	// Flush the writer to ensure all data is written to the buffer
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return buffer.Bytes(), nil
}

// ConcatCsvs concatenates multiple CSV files into a single CSV file.
// It uses the first CSV file as the header and appends the remaining CSV files.
func ConcatCSVs(csvs [][]byte) ([]byte, error) {
	if len(csvs) == 0 {
		// TODO: this should probably not be an error,
		// as it should be allowed to get a list of empty CSVs here,
		// in the case of only None responses from the API.
		return nil, fmt.Errorf("received empty CSV data")
	}

	parts := csvs
	if len(parts) == 1 {
		return parts[0], nil // Single CSV case
	}

	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	// Process the first CSV to get headers
	firstReader := csv.NewReader(bytes.NewReader(parts[0]))
	header, err := firstReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header from first CSV: %w", err)
	}

	// Check headers in all parts match the first one
	for i, part := range parts[1:] {
		if len(bytes.TrimSpace(part)) == 0 {
			continue
		}
		reader := csv.NewReader(bytes.NewReader(part))
		currentHeader, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read header from part %d: %w", i+2, err)
		}
		if len(currentHeader) != len(header) {
			return nil, fmt.Errorf("mismatched number of columns in part %d: expected %d, got %d", i+2, len(header), len(currentHeader))
		}
		for j, col := range header {
			if currentHeader[j] != col {
				return nil, fmt.Errorf("mismatched column name in part %d: expected '%s', got '%s' at position %d", i+2, col, currentHeader[j], j+1)
			}
		}
	}

	// Write header
	if err := writer.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Process all CSVs (including first one)
	for _, part := range parts {
		if len(bytes.TrimSpace(part)) == 0 {
			continue
		}

		reader := csv.NewReader(bytes.NewReader(part))
		// Skip header for each part (including first CSV)
		_, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to skip header: %w", err)
		}

		// Read and write all records
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read CSV record: %w", err)
			}

			if err := writer.Write(record); err != nil {
				return nil, fmt.Errorf("failed to write CSV record: %w", err)
			}
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return buffer.Bytes(), nil
}
