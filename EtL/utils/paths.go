package utils

import (
	"path/filepath"
	"runtime"
)

// ProjectRoot returns the absolute path to the project root directory
func ProjectRoot() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(b)))
}

// SQLPath returns the absolute path to a SQL file in the sql directory
func SQLPath(filename string) string {
	return filepath.Join(ProjectRoot(), "sql", filename)
}
