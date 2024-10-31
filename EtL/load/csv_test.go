package load

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConcatCSVs(t *testing.T) {
	tests := []struct {
		name           string
		csvData        [][]byte
		expectedOutput []byte
		expectedError  string
	}{
		{
			name: "Single CSV",
			csvData: [][]byte{[]byte(`id,name
1,Alice
2,Bob`)},
			expectedOutput: []byte(`id,name
1,Alice
2,Bob`),
			expectedError: "",
		},
		{
			name: "Multiple CSVs",
			csvData: [][]byte{
				[]byte(`id,name
1,Alice
2,Bob`),
				[]byte(`id,name
3,Charlie
4,David`),
			},
			expectedOutput: []byte(`id,name
1,Alice
2,Bob
3,Charlie
4,David
`),
			expectedError: "",
		},
		{
			name:           "Empty input",
			csvData:        [][]byte{},
			expectedOutput: nil,
			expectedError:  "received empty CSV data",
		},
		{
			name: "Invalid CSV format",
			csvData: [][]byte{
				[]byte(`id,name
1,Alice,extra`),
				[]byte(`id,name
2,Bob`),
			},
			expectedOutput: nil,
			expectedError:  "failed to read CSV record",
		},
		{
			name: "Empty CSV between valid ones",
			csvData: [][]byte{
				[]byte(`id,name
1,Alice`),
				[]byte(``),
				[]byte(`id,name
2,Bob`),
			},
			expectedOutput: []byte(`id,name
1,Alice
2,Bob
`),
			expectedError: "",
		},
		{
			name: "Mismatched columns",
			csvData: [][]byte{
				[]byte(`id,name
1,Alice`),
				[]byte(`id,email
2,bob@example.com`),
			},
			expectedOutput: nil,
			expectedError:  "mismatched column name in part 2: expected 'name', got 'email' at position 2",
		},
		{
			name: "Different number of columns",
			csvData: [][]byte{
				[]byte(`id,name
1,Alice`),
				[]byte(`id,name,email
2,Bob,bob@example.com`),
			},
			expectedOutput: nil,
			expectedError:  "mismatched number of columns in part 2: expected 2, got 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := ConcatCSVs(tt.csvData)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, string(tt.expectedOutput), string(output))
			}
		})
	}
}

func TestAddTickerColumn(t *testing.T) {
	tests := []struct {
		name           string
		csvData        []byte
		ticker         string
		expectedOutput []byte
		expectedError  string
	}{
		{
			name: "Valid CSV data",
			csvData: []byte(`id,name
1,Alice
2,Bob`),
			ticker: "AAPL",
			expectedOutput: []byte(`id,name,ticker
1,Alice,AAPL
2,Bob,AAPL
`),
			expectedError: "",
		},
		{
			name:           "Empty CSV data",
			csvData:        []byte(``),
			ticker:         "AAPL",
			expectedOutput: nil,
			expectedError:  "failed to read CSV header",
		},
		{
			name:    "CSV with no rows",
			csvData: []byte(`id,name`),
			ticker:  "AAPL",
			expectedOutput: []byte(`id,name,ticker
`),
			expectedError: "",
		},
		{
			name: "CSV with empty header",
			csvData: []byte(`,,,
1,Alice,30,Engineer
2,Bob,25,Designer`),
			ticker: "AAPL",
			expectedOutput: []byte(`,,,,ticker
1,Alice,30,Engineer,AAPL
2,Bob,25,Designer,AAPL
`),
			expectedError: "",
		},
		{
			name: "Invalid CSV data",
			csvData: []byte(`id,name
1,Alice
2,Bob
3,Charlie,30`), // Inconsistent number of columns
			ticker:         "AAPL",
			expectedOutput: nil,
			expectedError:  "failed to read CSV data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := AddTickerColumn(tt.csvData, tt.ticker)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedOutput, output)
			}
		})
	}
}
