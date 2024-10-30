package template

import (
	"bytes"
	"fmt"
	"os"
	"text/template"
)

// TODO: add unit tests

func ExecuteSqlTemplate(templatePath string, params map[string]any) (string, error) {
	// Read the template file
	content, err := os.ReadFile(templatePath)
	if err != nil {
		return "", err
	}

	// Parse and execute the template
	tmpl, err := template.New("sql").Parse(string(content))
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// ReadSqlTemplate reads a SQL template file and returns its contents as a string
func ReadSqlTemplate(templatePath string) (string, error) {
	content, err := os.ReadFile(templatePath)
	if err != nil {
		return "", fmt.Errorf("failed to read template file: %w", err)
	}
	return string(content), nil
}
