package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHalfOfSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		first    bool
		expected []string
	}{
		{
			name:     "empty slice",
			input:    []string{},
			first:    true,
			expected: []string{},
		},
		{
			name:     "single element first half",
			input:    []string{"AAPL"},
			first:    true,
			expected: []string{},
		},
		{
			name:     "single element second half",
			input:    []string{"AAPL"},
			first:    false,
			expected: []string{"AAPL"},
		},
		{
			name:     "even length first half",
			input:    []string{"AAPL", "MSFT", "GOOGL", "AMZN"},
			first:    true,
			expected: []string{"AAPL", "MSFT"},
		},
		{
			name:     "even length second half",
			input:    []string{"AAPL", "MSFT", "GOOGL", "AMZN"},
			first:    false,
			expected: []string{"GOOGL", "AMZN"},
		},
		{
			name:     "odd length first half",
			input:    []string{"AAPL", "MSFT", "GOOGL"},
			first:    true,
			expected: []string{"AAPL"},
		},
		{
			name:     "odd length second half",
			input:    []string{"AAPL", "MSFT", "GOOGL"},
			first:    false,
			expected: []string{"MSFT", "GOOGL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HalfOfSlice(tt.input, tt.first)
			assert.Equal(t, tt.expected, result)
		})
	}
}
