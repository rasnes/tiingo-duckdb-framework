package transform

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
