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
	t.ds.closeLock.RLock()
	err := t.tx.Commit()
	t.ds.closeLock.RUnlock()
	return err
}

func (t *transaction) Discard() {
	t.tx.Discard()
}

func (t *transaction) Delete(key ds.Key) error {
	err := t.tx.Delete(key.Bytes(), &opt.WriteOptions{Sync: false})
	return err
}
func (t *transaction) Get(key ds.Key) ([]byte, error) {
	val, err := t.tx.Get(key.Bytes(), nil)
	err = handleGetError(err)
	return val, err
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
	// so we only lock when closing, and invoking iterators (query & x commit)
	t.ds.closeLock.Lock()
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
	res, err := query(iter, q, qNaive)
	t.ds.closeLock.Unlock()
	return res, err
}

// NewTransaction returns a new transaction handler
func (d *Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	d.closeLock.RLock()
	tx, err := d.db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	txx := &transaction{d, tx}
	d.closeLock.RUnlock()
	return txx, nil
}
