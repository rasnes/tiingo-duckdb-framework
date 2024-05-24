package load

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
