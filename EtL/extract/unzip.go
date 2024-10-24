package extract

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
)

// UnzipSingleCSV takes a byte slice of a zip file and returns the contents of the single CSV file inside
func UnzipSingleCSV(zipData []byte) ([]byte, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, fmt.Errorf("failed to create zip reader: %w", err)
	}

	if len(zipReader.File) != 1 {
		return nil, fmt.Errorf("expected exactly one file in the zip archive, but found %d", len(zipReader.File))
	}

	file := zipReader.File[0]
	f, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", file.Name, err)
	}
	defer f.Close()

	content, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", file.Name, err)
	}

	return content, nil
}
