package postgres

import (
	"database/sql"
	"fmt"

	sqlds "github.com/RTradeLtd/go-datastores/sql"
	"go.uber.org/multierr"

	_ "github.com/lib/pq" //postgres driver
)

var (
	// RecreateTables indicates that you accept all consequences from dropping your table and recreating it
	RecreateTables = "recreate tables"
)

// Options are the postgres datastore options, reexported here for convenience.
type Options struct {
	// AcceptRecreateWarning is used as a safety check to pevent accidental deletion of existing tables and data
	// To accept the warning, you must set the value of this field to `recreate tables`, which can be done manually
	// or via the usage of the public `RecreateTables` variable
	AcceptRecreateWarning string
	Host                  string
	Port                  string
	User                  string
	Password              string
	Database              string
	Table                 string
	SSLMode               string
	RunMigrations         bool
	RecreateTables        bool
	CreateIndex           bool
}

// Queries are the postgres queries for a given table.
type Queries struct {
	deleteQuery  string
	existsQuery  string
	getQuery     string
	putQuery     string
	queryQuery   string
	prefixQuery  string
	limitQuery   string
	offsetQuery  string
	getSizeQuery string
}

// NewQueries creates a new PostgreSQL set of queries for the passed table
func NewQueries(tbl string) Queries {
	return Queries{
		deleteQuery:  fmt.Sprintf("DELETE FROM %s WHERE key = $1", tbl),
		existsQuery:  fmt.Sprintf("SELECT exists(SELECT 1 FROM %s WHERE key=$1)", tbl),
		getQuery:     fmt.Sprintf("SELECT data FROM %s WHERE key = $1", tbl),
		putQuery:     fmt.Sprintf("INSERT INTO %s (key, data) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET data = $2", tbl),
		queryQuery:   fmt.Sprintf("SELECT key, data FROM %s", tbl),
		prefixQuery:  ` WHERE key LIKE '%s%%' ORDER BY key`,
		limitQuery:   ` LIMIT %d`,
		offsetQuery:  ` OFFSET %d`,
		getSizeQuery: fmt.Sprintf("SELECT octet_length(data) FROM %s WHERE key = $1", tbl),
	}
}

// Delete returns the postgres query for deleting a row.
func (q Queries) Delete() string {
	return q.deleteQuery
}

// Exists returns the postgres query for determining if a row exists.
func (q Queries) Exists() string {
	return q.existsQuery
}

// Get returns the postgres query for getting a row.
func (q Queries) Get() string {
	return q.getQuery
}

// Put returns the postgres query for putting a row.
func (q Queries) Put() string {
	return q.putQuery
}

// Query returns the postgres query for getting multiple rows.
func (q Queries) Query() string {
	return q.queryQuery
}

// Prefix returns the postgres query fragment for getting a rows with a key prefix.
func (q Queries) Prefix() string {
	return q.prefixQuery
}

// Limit returns the postgres query fragment for limiting results.
func (q Queries) Limit() string {
	return q.limitQuery
}

// Offset returns the postgres query fragment for returning rows from a given offset.
func (q Queries) Offset() string {
	return q.offsetQuery
}

// GetSize returns the postgres query for determining the size of a value.
func (q Queries) GetSize() string {
	return q.getSizeQuery
}

// Create returns a datastore connected to postgres
func (opts *Options) Create() (*sqlds.Datastore, error) {
	opts.setDefaults()
	db, err := sql.Open("postgres", fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s password=%s sslmode=%s",
		opts.Host, opts.Port, opts.User, opts.Database, opts.Password, opts.SSLMode,
	))
	if err != nil {
		return nil, err
	}
	// only recreate the tables *IF* warning is accepted
	if opts.RecreateTables && opts.AcceptRecreateWarning == RecreateTables {
		if _, err := db.Exec(
			"DROP TABLE IF EXISTS blocks",
		); err != nil {
			return nil, err
		}
	}
	if opts.RunMigrations {
		if _, err := db.Exec(
			"CREATE TABLE IF NOT EXISTS blocks (key TEXT NOT NULL UNIQUE, data BYTEA NOT NULL)",
		); err != nil {
			return nil, multierr.Combine(err, db.Close())
		}
		if opts.CreateIndex {
			if _, err := db.Exec(
				fmt.Sprintf(
					"CREATE INDEX IF NOT EXISTS %s_key_text_pattern_ops_idx ON %s (key text_pattern_ops)",
					opts.Table, opts.Table,
				),
			); err != nil {
				return nil, multierr.Combine(err, db.Close())
			}

		}
	}
	return sqlds.NewDatastore(db, NewQueries(opts.Table)), nil
}

func (opts *Options) setDefaults() {
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}

	if opts.Port == "" {
		opts.Port = "5432"
	}

	if opts.User == "" {
		opts.User = "postgres"
	}

	if opts.Database == "" {
		opts.Database = "datastore"
	}

	if opts.Table == "" {
		opts.Table = "blocks"
	}
	if opts.SSLMode == "" {
		opts.SSLMode = "disable"
	}
}
