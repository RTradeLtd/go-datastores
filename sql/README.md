# SQL Datastore

`sqlds` allows using arbitrary SQL databases as an IPFS datastore. It defines a set of functionality that can be used by any golang client that satisfies the `database/sql` interface. Included in an implementation for the PostgreSQL database. This datastore is forked from `ipfs/go-ds-sql` with a few modifications, and cleanup to the codebase, consisting of:

* Enable table recreation, migration, and index creation as an optional capability when using PostgreSQL
* Change testing scheme to ensure that the postgres functionality is tested and subsequently the datastore implementation is tested
  * Upstream doesn't specifically test the postgres implementation which is likely to introduce bugs.
* Use the `uber-go/multierr` package to combine errors where appropriatey
    * In transaction mode if any errors happen, the error returned from the rollback was not returned, instead we combine the error for better visibility

# Usage (Postgres)

## Easy (Automatic)


The `postgres` datastore has some helper utility to enable automatic table and index creation, as well as the ability to recreate the table by dropping it, and creating it. Note that this setting requires `RunMigrations` also set to true. If not the `CreateIndex` setting will be ignored. Additionally note that we will never delete tables unless specified.

Note that we will never delete tables automatically, and tables will only be deleted when `RecreateTables` is set to true, and `AcceptRecreateWarning` is set to the value of `RecreateTables`. Additionally you must make sure to set `RunMigrations` otherwise we wont actually create the tables, and instead just delete the existing tables.

The following options can be used to enable table creation, and index creation:


```Go
import (
    pgds "github.com/RTradeLtd/go-datastores/sql/postgres"
    "github.com/ipfs/go-datastore"
)
opts := &pgds.Options{
	Host:                  "127.0.0.1",
	Port:                  "5432",
	User:                  "postgres",
	Database:              "datastores",
	Password:              "password123",
    SSLMode:               "disable",
    Table:                  "blocks",
    // this will drop the existing table
	AcceptRecreateWarning: pgds.RecreateTables,
	RunMigrations:         true,
	RecreateTables:        true,
	CreateIndex:           true,
}
ds, err := opts.Create()
if err != nil {
	log.Fatal(err)
}
if err := ds.Put(
    datastore.NewKey("much test very wow"), 
    []byte("whose even going to read this anyways"),
); err != nil {
    log.Fatal(err)
}
```

## Hard (Manual)

Ensure a database is created and a table exists with `key` and `data` columns. For example, in PostgreSQL you can create a table with the following structure (replacing `table_name` with the name of the table the datastore will use - by default this is `blocks`):

```sql
CREATE TABLE IF NOT EXISTS table_name (key TEXT NOT NULL UNIQUE, data BYTEA)
```

It's recommended to create an index on the `key` column that is optimised for prefix scans. For example, in PostgreSQL you can create a `text_pattern_ops` index on the table:

```sql
CREATE INDEX IF NOT EXISTS table_name_key_text_pattern_ops_idx ON table_name (key text_pattern_ops)
```

Import and use in your application:

```go
import (
	"database/sql"
	"github.com/RTradeLtd/go-datastores/sql"
	pg "github.com/RTradeLtd/go-datastores/sql/postgres"
)

mydb, _ := sql.Open("yourdb", "yourdbparameters")

// Implement the Queries interface for your SQL impl.
// ...or use the provided PostgreSQL queries
queries := pg.NewQueries("blocks")

ds := sqlds.NewDatastore(mydb, queries)
```

# license

As this is forked from upstream, it retains its existing license which can be found in `LICENSE.orig`.