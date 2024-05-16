package extract

import (
	"database/sql"
	_ "github.com/marcboeker/go-duckdb"
	"log/slog"
)

type DuckDB struct {
	Logger *slog.Logger
	DuckDB *sql.DB
	Path   string
}

func NewDuckDB(path string, logger *slog.Logger) (*DuckDB, error) {

	// TODO: rewrite use Config struct instead of path string.
	// TODO: rewrite to use the sql.OpenDB(connector) instead, as this enables
	// the appender API as well.
	// TODO: enable support for motherduck by checking what the path starts with.
	// If starting with `md:`, then a token should loaded from ENV and appended to the string.

	var pathOut string
	if path == "" || path == ":memory:" {
		logger.Info("Creating in-memory DuckDB database.")
		path = ""
		pathOut = ":memory:"
	} else {
		logger.Info("Creating or attaching DuckDB database at %s", path)
		pathOut = path
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	return &DuckDB{
		Logger: logger,
		DuckDB: db,
		Path:   pathOut,
	}, nil
}

func (d *DuckDB) Close() {
	d.DuckDB.Close()
}
