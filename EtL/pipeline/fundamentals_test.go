package pipeline

import (
	"database/sql"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockDB struct {
	loadCSVWithQueryFunc func([]byte, string, map[string]any) (sql.Result, error)
}

type mockDBWithResults struct {
	results     map[string][]string
	loadCSVFunc func([]byte, string, bool) error
}

func (m *mockDBWithResults) GetQueryResults(query string) (map[string][]string, error) {
	return m.results, nil
}

func (m *mockDBWithResults) LoadCSV(csv []byte, table string, truncate bool) error {
	if m.loadCSVFunc != nil {
		return m.loadCSVFunc(csv, table, truncate)
	}
	return nil
}

type mockTiingoClientDaily struct {
	getDailyFunc func(ticker string) ([]byte, error)
}

func (m *mockTiingoClientDaily) GetDailyFundamentals(ticker string) ([]byte, error) {
	if m.getDailyFunc != nil {
		return m.getDailyFunc(ticker)
	}
	return []byte("date,marketCap\n2024-01-01,100000"), nil
}

func (m *mockDB) LoadCSVWithQuery(csv []byte, queryTemplate string, params map[string]any) (sql.Result, error) {
	if m.loadCSVWithQueryFunc != nil {
		return m.loadCSVWithQueryFunc(csv, queryTemplate, params)
	}
	return &mockSQLResult{rowsAffected: 0}, nil
}

type mockSQLResult struct {
	rowsAffected int64
}

func (m mockSQLResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (m mockSQLResult) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

type mockTiingoClient struct {
	getMetaFunc func(string) ([]byte, error)
}

func (m *mockTiingoClient) GetMeta(tickers string) ([]byte, error) {
	if m.getMetaFunc != nil {
		return m.getMetaFunc(tickers)
	}
	return []byte("ticker,name\nAAPL,Apple Inc\nGOOGL,Alphabet Inc"), nil
}

func TestDailyFundamentals(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name              string
		mockDBResults     map[string][]string
		mockDBError       error
		mockDailyData     []byte
		mockDailyError    error
		mockLoadError     error
		wantErr          bool
		errContains      string
	}{
		{
			name: "successful daily fundamentals update",
			mockDBResults: map[string][]string{
				"ticker": {"AAPL", "GOOGL"},
			},
			mockDailyData: []byte("date,marketCap\n2024-01-01,100000"),
			wantErr:      false,
		},
		{
			name: "error getting tickers",
			mockDBResults: map[string][]string{
				"ticker": {},
			},
			wantErr:      true,
			errContains:  "no tickers found in selected_fundamentals results",
		},
		{
			name: "error fetching daily fundamentals",
			mockDBResults: map[string][]string{
				"ticker": {"AAPL"},
			},
			mockDailyError: errors.New("API error"),
			wantErr:       true,
			errContains:   "error fetching daily fundamentals",
		},
		{
			name: "error loading to DB",
			mockDBResults: map[string][]string{
				"ticker": {"AAPL"},
			},
			mockDailyData: []byte("date,marketCap\n2024-01-01,100000"),
			mockLoadError: errors.New("load error"),
			wantErr:      true,
			errContains:  "error loading daily fundamentals to DB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &mockDBWithResults{
				results: tt.mockDBResults,
				loadCSVFunc: func(csv []byte, table string, truncate bool) error {
					return tt.mockLoadError
				},
			}

			mockClient := &mockTiingoClientDaily{
				getDailyFunc: func(ticker string) ([]byte, error) {
					if tt.mockDailyError != nil {
						return nil, tt.mockDailyError
					}
					return tt.mockDailyData, nil
				},
			}

			_, err := DailyFundamentals(mockDB, mockClient, logger, "template")

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdateMetadata(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name              string
		mockDBResults     map[string][]string
		mockDBError       error
		mockMetaData      []byte
		mockMetaError     error
		mockLoadError     error
		mockRowsAffected  int64
		mockTemplateError error
		wantCount         int
		wantErr          bool
		errContains      string
	}{
		{
			name: "successful metadata update",
			mockDBResults: map[string][]string{
				"tickers": {"AAPL,GOOGL"},
			},
			mockMetaData:  []byte("ticker,name\nAAPL,Apple Inc\nGOOGL,Alphabet Inc"),
			wantCount:     2,
			wantErr:       false,
		},
		{
			name:          "error getting tickers",
			mockMetaError: errors.New("database error"),
			wantCount:     0,
			wantErr:       true,
			errContains:   "error fetching metadata from Tiingo",
		},
		{
			name: "error fetching metadata",
			mockDBResults: map[string][]string{
				"tickers": {"AAPL,GOOGL"},
			},
			mockMetaError: errors.New("API error"),
			wantCount:     0,
			wantErr:       true,
			errContains:   "error fetching metadata",
		},
		{
			name: "error loading metadata",
			mockDBResults: map[string][]string{
				"tickers": {"AAPL,GOOGL"},
			},
			mockMetaData:  []byte("ticker,name\nAAPL,Apple Inc\nGOOGL,Alphabet Inc"),
			mockLoadError: errors.New("load error"),
			wantCount:     0,
			wantErr:       true,
			errContains:   "error loading metadata",
		},
		{
			name: "no tickers need update",
			mockDBResults: map[string][]string{
				"tickers": {""},
			},
			wantCount: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := &mockDB{
				loadCSVWithQueryFunc: func(csv []byte, queryTemplate string, params map[string]any) (sql.Result, error) {
					if tt.mockLoadError != nil {
						return nil, tt.mockLoadError
					}
					// For successful case, return 2 rows affected to match test expectation
					if tt.name == "successful metadata update" {
						return &mockSQLResult{rowsAffected: 2}, nil
					}
					return &mockSQLResult{rowsAffected: tt.mockRowsAffected}, nil
				},
			}

			mockClient := &mockTiingoClient{
				getMetaFunc: func(tickers string) ([]byte, error) {
					if tt.mockMetaError != nil {
						return nil, tt.mockMetaError
					}
					return tt.mockMetaData, nil
				},
			}


			// Use a template string directly for testing
			templateContent := "INSERT INTO fundamentals_meta SELECT * FROM read_csv('{{.CsvFile}}');"
			gotCount, err := UpdateMetadata(mockDB, mockClient, logger, templateContent)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, gotCount)
		})
	}
}
