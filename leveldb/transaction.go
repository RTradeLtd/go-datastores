package leveldb

import (
	ds "github.com/ipfs/go-datastore"

	dsq "github.com/ipfs/go-datastore/query"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type transaction struct {
	ds *Datastore
	tx *leveldb.Transaction
}

func (t *transaction) Commit() error {
	return t.tx.Commit()
}

func (t *transaction) Discard() {
	t.tx.Discard()
}

func (t *transaction) Delete(key ds.Key) error {
	return t.tx.Delete(key.Bytes(), &opt.WriteOptions{Sync: false})
}
func (t *transaction) Get(key ds.Key) ([]byte, error) {
	val, err := t.tx.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return val, nil
}

func (t *transaction) Has(key ds.Key) (bool, error) {
	return ds.GetBackedHas(t, key)
}

func (t *transaction) GetSize(key ds.Key) (size int, err error) {
	return ds.GetBackedSize(t, key)
}
func (t *transaction) Put(key ds.Key, value []byte) (err error) {
	return t.tx.Put(key.Bytes(), value, &opt.WriteOptions{Sync: false})
}

func (t *transaction) Query(q dsq.Query) (dsq.Results, error) {
	// so we only lock when closing, and invoking iterators (query)
	t.ds.closeLock.Lock()
	defer t.ds.closeLock.Unlock()
	var rnge *util.Range
	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		rnge = util.BytesPrefix([]byte(prefix + "/"))
		qNaive.Prefix = ""
	}
	iter := t.tx.NewIterator(rnge, nil)
	return query(iter, q, qNaive)
}

// NewTransaction returns a new transaction handler
func (d *Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	tx, err := d.db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	return &transaction{d, tx}, nil
}
