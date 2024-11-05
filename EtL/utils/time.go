package utils

import "time"

// TimeProvider interface for time operations
type TimeProvider interface {
	Now() time.Time
}

// RealTimeProvider implements TimeProvider using actual system time
type RealTimeProvider struct{}

func (p RealTimeProvider) Now() time.Time {
	return time.Now()
}
