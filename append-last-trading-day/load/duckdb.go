package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/rasnes/tiingo-duckdb-framework/append-last-trading-day/config"
	"io"
	"log/slog"
	"os"
	"strings"
)

type DuckDB struct {
	Logger    *slog.Logger
	DB        *sql.DB
	Connector *duckdb.Connector
	Conn      driver.Conn
	Appender  *duckdb.Appender
	DBType    string
}

func NewDuckDB(config *config.Config, logger *slog.Logger) (*DuckDB, error) {
	var path string
	var dbType string
	if strings.HasPrefix(config.DuckDB.Path, "md:") {
		motherduckToken := os.Getenv("MOTHERDUCK_TOKEN")
		if motherduckToken == "" {
			return nil, fmt.Errorf("MOTHERDUCK_TOKEN env variable is not set")
		}
		path = fmt.Sprintf("md:?motherduck_token=%s", motherduckToken)
		dbType = ":md:"
	} else if config.DuckDB.Path == "" || config.DuckDB.Path == ":memory:" {
		path = ""
		dbType = ":memory:"
	} else {
		path = config.DuckDB.Path
		dbType = path
	}

	var connInitFn func(driver.ExecerContext) error
	if len(config.DuckDB.ConnInitFnQueries) == 0 {
		connInitFn = nil
	} else {
		connInitFn = func(exec driver.ExecerContext) error {
			for _, path := range config.DuckDB.ConnInitFnQueries {
				query, err := readQuery(path)
				if err != nil {
					return err
				}

				// Execute the query read from the file
				_, err = exec.ExecContext(context.Background(), string(query), nil)
				if err != nil {
					return fmt.Errorf("failed to execute query from file %s: %w", path, err)
				}
			}
			return nil
		}
		logger.Debug(fmt.Sprintf("Connection initialization queries: %v", config.DuckDB.ConnInitFnQueries))
	}

	connector, err := duckdb.NewConnector(path, connInitFn)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	switch dbType {
	case ":memory:":
		logger.Info("Connected to DuckDB in-memory database")
	case ":md:":
		logger.Info("Connected to MotherDuck database")
	default:
		logger.Info(fmt.Sprintf("Connected to local DuckDB database at %s", dbType))
	}

	return &DuckDB{
		Logger:    logger,
		DB:        db,
		Connector: connector,
		DBType:    dbType,
	}, nil
}

func readQuery(path string) ([]byte, error) {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	// Read the content of the file
	query, err := io.ReadAll(file)
	if err != nil {
		file.Close() // Ensure the file is closed if reading fails
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	// Close the file after reading its content
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close file %s: %w", path, err)
	}
	return query, nil
}

func (db *DuckDB) Close() {
	db.DB.Close()
	db.Connector.Close()
}

func (db *DuckDB) LoadCSV(csv []byte, table string, insert bool) error {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "tmp.csv")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write the CSV data to the temporary file
	if _, err := tmpFile.Write(csv); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write to temporary file: %w", err)
	}

	// Close the file to flush the data
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// TODO: add support for also using the INSERT OR REPLACE INTO statement
	// Use the COPY statement to read the data from the temporary file into DuckDB
	var query string
	if insert {
		query = fmt.Sprintf("INSERT OR REPLACE INTO %s SELECT * FROM read_csv('%s');", table, tmpFile.Name())
	} else {
		query = fmt.Sprintf("COPY %s FROM '%s' (FORMAT CSV, HEADER);", table, tmpFile.Name())
	}
	if _, err := db.DB.ExecContext(context.Background(), query); err != nil {
		return fmt.Errorf("failed to execute COPY or INSERT OR REPLACE INTO statement: %w", err)
	}

	return nil
}

func (db *DuckDB) RunQuery(query string) error {
	_, err := db.DB.ExecContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	return nil
}

func (db *DuckDB) RunQueryFile(path string) error {
	query, err := readQuery(path)
	if err != nil {
		return err
	}

	return db.RunQuery(string(query))
}

func (db *DuckDB) GetQueryResultsFromFile(path string) (map[string][]string, error) {
	query, err := readQuery(path)
	if err != nil {
		return nil, err
	}

	return db.GetQueryResults(string(query))
}

// GetQueryResults executes a query and returns the results as a map of column names to slices of values
func (db *DuckDB) GetQueryResults(query string) (map[string][]string, error) {
	// Execute the query
	rows, err := db.DB.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Initialize a map to hold slices for each column
	results := make(map[string][]string)
	for _, col := range columns {
		results[col] = []string{}
	}

	// Iterate over the rows
	for rows.Next() {
		// Create a slice to hold the column values
		values := make([]interface{}, len(columns))
		// Create a slice of pointers to the column values
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append the values to the corresponding slices in the results map
		for i, col := range columns {
			// Convert the value to a string
			valueStr := fmt.Sprintf("%v", values[i])
			results[col] = append(results[col], valueStr)
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return results, nil
}
