package pipeline

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockHistoryFetcher implements the historyFetcher interface for testing
type mockHistoryFetcher struct {
	getHistoryFunc func(ticker string) ([]byte, error)
}

func (m *mockHistoryFetcher) GetHistory(ticker string) ([]byte, error) {
	return m.getHistoryFunc(ticker)
}

// mockCSVLoader implements the csvLoader interface for testing
type mockCSVLoader struct {
	loadCSVFunc func(csv []byte, table string, insert bool) error
}

func (m *mockCSVLoader) LoadCSV(csv []byte, table string, insert bool) error {
	return m.loadCSVFunc(csv, table, insert)
}

func TestBackfillEndOfDay(t *testing.T) {
	logger := slog.Default()

	tests := []struct {
		name           string
		tickers        []string
		mockGetHistory func(ticker string) ([]byte, error)
		mockLoadCSV    func(csv []byte, table string, insert bool) error
		wantCount      int
		wantErr        bool
		errContains    string
	}{
		{
			name:    "successful backfill of multiple tickers",
			tickers: []string{"AAPL", "GOOGL"},
			mockGetHistory: func(ticker string) ([]byte, error) {
				return []byte("date,close\n2024-01-01,100.0"), nil
			},
			mockLoadCSV: func(csv []byte, table string, insert bool) error {
				return nil
			},
			wantCount: 2,
			wantErr:   false,
		},
		{
			name:    "handles GetHistory error",
			tickers: []string{"AAPL", "GOOGL"},
			mockGetHistory: func(ticker string) ([]byte, error) {
				if ticker == "AAPL" {
					return nil, errors.New("API error")
				}
				return []byte("date,close\n2024-01-01,100.0"), nil
			},
			mockLoadCSV: func(csv []byte, table string, insert bool) error {
				return nil
			},
			wantCount:   1,
			wantErr:     true,
			errContains: "error fetching history for ticker AAPL",
		},
		{
			name:    "handles LoadCSV error",
			tickers: []string{"AAPL", "GOOGL"},
			mockGetHistory: func(ticker string) ([]byte, error) {
				return []byte("date,close\n2024-01-01,100.0"), nil
			},
			mockLoadCSV: func(csv []byte, table string, insert bool) error {
				if table == "daily_adjusted" {
					return errors.New("database error")
				}
				return nil
			},
			wantCount:   0,
			wantErr:     true,
			errContains: "error loading history to DB",
		},
		{
			name:    "empty ticker list",
			tickers: []string{},
			mockGetHistory: func(ticker string) ([]byte, error) {
				return []byte("date,close\n2024-01-01,100.0"), nil
			},
			mockLoadCSV: func(csv []byte, table string, insert bool) error {
				return nil
			},
			wantCount: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockHistoryFetcher{
				getHistoryFunc: tt.mockGetHistory,
			}
			mockDB := &mockCSVLoader{
				loadCSVFunc: tt.mockLoadCSV,
			}

			gotCount, err := BackfillEndOfDay(tt.tickers, mockClient, logger, mockDB)

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
