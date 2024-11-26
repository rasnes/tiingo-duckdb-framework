package load

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestRemoveDuplicateRows(t *testing.T) {
	tests := []struct {
		name           string
		csvData        []byte
		expectedOutput []byte
		expectedError  string
	}{
		{
			name: "No duplicates",
			csvData: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
2023-06-30,2023,2,overview,score,4.0
2023-07-01,2023,2,foo,bar,99.0`),
			expectedOutput: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
2023-06-30,2023,2,overview,score,4.0
2023-07-01,2023,2,foo,bar,99.0
`),
			expectedError: "",
		},
		{
			name: "With duplicates",
			csvData: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
2023-03-31,2023,1,overview,score,3.0
2023-06-30,2023,2,overview,score,4.0`),
			expectedOutput: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
2023-06-30,2023,2,overview,score,4.0
`),
			expectedError: "",
		},
		{
			name: "All rows duplicate",
			csvData: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
2023-03-31,2023,1,overview,score,3.0`),
			expectedOutput: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
`),
			expectedError: "",
		},
		{
			name:           "Empty CSV",
			csvData:        []byte(``),
			expectedOutput: nil,
			expectedError:  "received empty CSV data",
		},
		{
			name: "Only header",
			csvData: []byte(`date,year,quarter,type,metric,value
`),
			expectedOutput: []byte(`date,year,quarter,type,metric,value
`),
			expectedError: "",
		},
		{
			name: "Invalid CSV format",
			csvData: []byte(`date,year,quarter,type,metric,value
2023-03-31,2023,1,overview,score,3.0
2023-03-31,2023,1,overview,score,3.0,extra`),
			expectedOutput: nil,
			expectedError:  "failed to read CSV record",
		},
		{
			name: "Duplicates with special characters",
			csvData: []byte(`date,name,description
2023-03-31,"Smith, John","Description, with, commas"
2023-03-31,"Smith, John","Description, with, commas"
2023-06-30,"Doe, Jane","Another, description"`),
			expectedOutput: []byte(`date,name,description
2023-03-31,"Smith, John","Description, with, commas"
2023-06-30,"Doe, Jane","Another, description"
`),
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := RemoveDuplicateRows(tt.csvData)
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
