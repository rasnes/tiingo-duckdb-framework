package extract

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
				// Open the file
				file, err := os.Open(path)
				if err != nil {
					return fmt.Errorf("failed to open file %s: %w", path, err)
				}

				// Read the content of the file
				query, err := io.ReadAll(file)
				if err != nil {
					file.Close() // Ensure the file is closed if reading fails
					return fmt.Errorf("failed to read file %s: %w", path, err)
				}

				// Close the file after reading its content
				if err := file.Close(); err != nil {
					return fmt.Errorf("failed to close file %s: %w", path, err)
				}

				// Execute the query read from the file
				_, err = exec.ExecContext(context.Background(), string(query), nil)
				if err != nil {
					return fmt.Errorf("failed to execute query from file %s: %w", path, err)
				}
			}
			return nil
		}
		logger.Debug("Connection initialization queries: %v", config.DuckDB.ConnInitFnQueries)
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
		logger.Info("Connected to local DuckDB database at %s", dbType)
	}

	return &DuckDB{
		Logger:    logger,
		DB:        db,
		Connector: connector,
		DBType:    dbType,
	}, nil
}

func (db *DuckDB) Close() {
	db.DB.Close()
	db.Connector.Close()
}

func (db *DuckDB) LoadCSV(csv []byte, table string) error {
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

	// Use the COPY statement to read the data from the temporary file into DuckDB
	query := fmt.Sprintf("COPY %s FROM '%s' (FORMAT CSV, HEADER)", table, tmpFile.Name())
	if _, err := db.DB.ExecContext(context.Background(), query); err != nil {
		return fmt.Errorf("failed to execute COPY statement: %w", err)
	}

	return nil
}
