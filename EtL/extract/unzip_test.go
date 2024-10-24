package extract

import (
	"archive/zip"
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func createZipFile(files map[string][]byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	for name, content := range files {
		writer, err := zipWriter.Create(name)
		if err != nil {
			return nil, err
		}
		_, err = writer.Write(content)
		if err != nil {
			return nil, err
		}
	}

	err := zipWriter.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func TestUnzipSingleCSV_ValidZipWithSingleCSV(t *testing.T) {
	csvContent := []byte("id,name\n1,Alice\n2,Bob")
	zipData, err := createZipFile(map[string][]byte{"file.csv": csvContent})
	assert.NoError(t, err)

	result, err := UnzipSingleCSV(zipData)
	assert.NoError(t, err)
	assert.Equal(t, csvContent, result)
}

func TestUnzipSingleCSV_EmptyZip(t *testing.T) {
	zipData, err := createZipFile(map[string][]byte{})
	assert.NoError(t, err)

	_, err = UnzipSingleCSV(zipData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly one file in the zip archive")
}

func TestUnzipSingleCSV_MultipleFilesInZip(t *testing.T) {
	zipData, err := createZipFile(map[string][]byte{
		"file1.csv": []byte("id,name\n1,Alice"),
		"file2.csv": []byte("id,name\n2,Bob"),
	})
	assert.NoError(t, err)

	_, err = UnzipSingleCSV(zipData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly one file in the zip archive")
}

func TestUnzipSingleCSV_InvalidZip(t *testing.T) {
	invalidZipData := []byte("this is not a valid zip file")

	_, err := UnzipSingleCSV(invalidZipData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create zip reader")
}
