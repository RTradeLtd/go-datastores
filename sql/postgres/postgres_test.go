package postgres

import (
	"strings"
	"testing"
)

func TestPostgres_Queries(t *testing.T) {
	tableName := "querytabletest"
	queries := NewQueries(tableName)
	if !strings.Contains(queries.deleteQuery, tableName) {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.existsQuery, tableName) {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.getQuery, tableName) {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.putQuery, tableName) {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.queryQuery, tableName) {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.prefixQuery, "WHERE key LIKE") {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.limitQuery, "LIMIT") {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.offsetQuery, "OFFSET") {
		t.Fatal("bad query")
	}
	if !strings.Contains(queries.getSizeQuery, tableName) {
		t.Fatal("bad query")
	}

}

func TestSetDefaultOptions(t *testing.T) {
	opts := &Options{}
	opts.setDefaults()
	if opts.Host != "127.0.0.1" {
		t.Fatal("bad host")
	}
	if opts.Port != "5432" {
		t.Fatal("bad ports")
	}
	if opts.User != "postgres" {
		t.Fatal("bad user")
	}
	if opts.Database != "datastore" {
		t.Fatal("bad database")
	}
	if opts.Table != "blocks" {
		t.Fatal("bad table")
	}
	if opts.SSLMode != "disable" {
		t.Fatal("badd sslmode")
	}
	if opts.RunMigrations {
		t.Fatal("run migrations should be false")
	}
}

func TestCreate(t *testing.T) {
	opts := &Options{
		Host:          "127.0.0.1",
		Port:          "5432",
		User:          "postgres",
		Database:      "datastores",
		Password:      "password123",
		SSLMode:       "disable",
		RunMigrations: true,
	}
	ds, err := opts.Create()
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.Close(); err != nil {
		t.Fatal(err)
	}
}
