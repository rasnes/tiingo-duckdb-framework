package template

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecuteSqlTemplate(t *testing.T) {
	// Create a temporary template file
	tmpFile, err := os.CreateTemp("", "test_template.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write test template content
	templateContent := "SELECT * FROM {{.TableName}} WHERE id = {{.ID}};"
	_, err = tmpFile.WriteString(templateContent)
	assert.NoError(t, err)
	tmpFile.Close()

	tests := []struct {
		name       string
		params     map[string]any
		want       string
		wantErr    bool
		errMessage string
	}{
		{
			name: "successful template execution",
			params: map[string]any{
				"TableName": "users",
				"ID":        123,
			},
			want:    "SELECT * FROM users WHERE id = 123;",
			wantErr: false,
		},
		{
			name:    "missing parameter",
			params:  map[string]any{},
			want:    "SELECT * FROM <no value> WHERE id = <no value>;",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ExecuteSqlTemplate(tmpFile.Name(), tt.params)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, result)
			}
		})
	}
}

func TestReadSqlTemplate(t *testing.T) {
	// Create a temporary template file
	tmpFile, err := os.CreateTemp("", "test_template.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write test content
	content := "SELECT * FROM test_table;"
	_, err = tmpFile.WriteString(content)
	assert.NoError(t, err)
	tmpFile.Close()

	tests := []struct {
		name       string
		filepath   string
		want       string
		wantErr    bool
		errMessage string
	}{
		{
			name:     "successful read",
			filepath: tmpFile.Name(),
			want:     content,
			wantErr:  false,
		},
		{
			name:       "file not found",
			filepath:   "nonexistent.sql",
			wantErr:    true,
			errMessage: "no such file or directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ReadSqlTemplate(tt.filepath)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, result)
			}
		})
	}
}
