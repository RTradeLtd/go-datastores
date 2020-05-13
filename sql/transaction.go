package sqlds

import (
	"database/sql"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"go.uber.org/multierr"
)

type txn struct {
	db      *sql.DB
	queries Queries
	txn     *sql.Tx
}

// NewTransaction creates a new database transaction, note the readOnly parameter is ignored by this implementation.
func (ds *Datastore) NewTransaction(_ bool) (ds.Txn, error) {
	sqlTxn, err := ds.db.Begin()
	if err != nil {
		if sqlTxn != nil {
			// wrap errors
			err = multierr.Combine(err, sqlTxn.Rollback())
		}
		return nil, err
	}

	return &txn{
		db:      ds.db,
		queries: ds.queries,
		txn:     sqlTxn,
	}, nil
}

func (t *txn) Get(key ds.Key) ([]byte, error) {
	var out []byte
	row := t.txn.QueryRow(t.queries.Get(), key.String())

	switch err := row.Scan(&out); err {
	case sql.ErrNoRows:
		return nil, ds.ErrNotFound
	case nil:
		return out, nil
	default:
		return nil, err
	}
}

func (t *txn) Has(key ds.Key) (bool, error) {
	var exists bool
	row := t.txn.QueryRow(t.queries.Exists(), key.String())

	switch err := row.Scan(&exists); err {
	case sql.ErrNoRows:
		return exists, nil
	case nil:
		return exists, nil
	default:
		return exists, err
	}
}

func (t *txn) GetSize(key ds.Key) (int, error) {
	var size int
	row := t.txn.QueryRow(t.queries.GetSize(), key.String())

	switch err := row.Scan(&size); err {
	case sql.ErrNoRows:
		return -1, ds.ErrNotFound
	case nil:
		return size, nil
	default:
		return 0, err
	}
}

func (t *txn) Query(q dsq.Query) (dsq.Results, error) {
	return nil, ErrNotImplemented
}

// Put adds a value to the datastore identified by the given key.
func (t *txn) Put(key ds.Key, val []byte) error {
	_, err := t.txn.Exec(t.queries.Put(), key.String(), val)
	if err != nil {
		err = multierr.Combine(err, t.txn.Rollback())
	}
	return err
}

// Delete removes a value from the datastore that matches the given key.
func (t *txn) Delete(key ds.Key) error {
	_, err := t.txn.Exec(t.queries.Delete(), key.String())
	if err != nil {
		err = multierr.Combine(err, t.txn.Rollback())
	}
	return err
}

// Commit finalizes a transaction.
func (t *txn) Commit() error {
	err := t.txn.Commit()
	if err != nil {
		err = multierr.Combine(err, t.txn.Rollback())
	}
	return err
}

// Discard throws away changes recorded in a transaction without committing
// them to the underlying Datastore.
func (t *txn) Discard() {
	_ = t.txn.Rollback()
}
