package utils

// HalfOfSlice returns the first or second half of a slice of strings
// Useful for splittng a slice of tickers into two halves, each
// of which could be scheduled on separate clock hours.
// This is useful as simple workaround for Tiingo's 10k requests per hour.
func HalfOfSlice(slice []string, first bool) []string {
	if len(slice) == 0 {
		return slice
	}

	if first {
		return slice[:len(slice)/2]
	}

	return slice[len(slice)/2:]
}
