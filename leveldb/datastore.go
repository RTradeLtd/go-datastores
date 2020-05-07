package leveldb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/atomic"
)

var (
	_ ds.Datastore    = (*Datastore)(nil)
	_ ds.TxnDatastore = (*Datastore)(nil)
	// ErrClosed is an error message returned when the datastore is no longer open
	ErrClosed = errors.New("datastore closed")
)

// Datastore is a go-datastore implement using leveldb
type Datastore struct {
	db         *leveldb.DB
	path       string
	closed     *atomic.Bool
	syncWrites bool
	close      sync.Once
	closeLock  sync.Mutex
}

// Options is an alias of syndtr/goleveldb/opt.Options which might be extended
// in the future.
type Options = opt.Options

// NewDatastore returns a new datastore backed by leveldb
func NewDatastore(path string, opts *Options) (*Datastore, error) {
	noSync := opts.NoSync
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	ds := Datastore{
		//accessor: &accessor{ldb: db, syncWrites: true, closeLk: new(sync.RWMutex)},
		db:         db,
		path:       path,
		closed:     atomic.NewBool(false),
		syncWrites: noSync == false,
	}
	return &ds, nil
}

// Put stores a key-value pair in leveldb
func (d *Datastore) Put(key ds.Key, value []byte) (err error) {
	if d.closed.Load() {
		return ErrClosed
	}
	return d.db.Put(key.Bytes(), value, &opt.WriteOptions{Sync: d.syncWrites})
}

// Sync is a noop
func (d *Datastore) Sync(prefix ds.Key) error {
	return nil
}

// Get returns the value corresponding to the key
func (d *Datastore) Get(key ds.Key) (value []byte, err error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	val, err := d.db.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return val, nil
}

// Has returns whether or not we have the key
func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	if d.closed.Load() {
		return false, ErrClosed
	}
	return ds.GetBackedHas(d, key)
}

// GetSize returns the size of the associated key
func (d *Datastore) GetSize(key ds.Key) (size int, err error) {
	if d.closed.Load() {
		return 0, ErrClosed
	}
	return ds.GetBackedSize(d, key)
}

// Delete removed the key from our datastore
func (d *Datastore) Delete(key ds.Key) (err error) {
	if d.closed.Load() {
		return ErrClosed
	}
	return d.db.Delete(key.Bytes(), &opt.WriteOptions{Sync: d.syncWrites})
}

// Query searches for keys in our datastore
func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	// closing is only unsafe when there are pending iterators
	// so we only lock when closing, and invoking iterators (query)
	d.closeLock.Lock()
	defer d.closeLock.Unlock()
	var rnge *util.Range

	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		rnge = util.BytesPrefix([]byte(prefix + "/"))
		qNaive.Prefix = ""
	}
	i := d.db.NewIterator(rnge, nil)
	next := i.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			qNaive.Orders = nil
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			next = func() bool {
				next = i.Prev
				return i.Last()
			}
			qNaive.Orders = nil
		default:
		}
	}
	r := dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			if !next() {
				return dsq.Result{}, false
			}
			k := string(i.Key())
			e := dsq.Entry{Key: k, Size: len(i.Value())}

			if !q.KeysOnly {
				buf := make([]byte, len(i.Value()))
				copy(buf, i.Value())
				e.Value = buf
			}
			return dsq.Result{Entry: e}, true
		},
		Close: func() error {
			i.Release()
			return nil
		},
	})
	return dsq.NaiveQueryApply(qNaive, r), nil
}

// DiskUsage returns the current disk size used by this levelDB.
// For in-mem datastores, it will return 0.
func (d *Datastore) DiskUsage() (du uint64, err error) {
	if d.closed.Load() {
		return 0, ErrClosed
	}
	err = filepath.Walk(d.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		du += uint64(info.Size())
		return nil
	})
	return
}

// Close shuts down leveldb
func (d *Datastore) Close() (err error) {
	if d.closed.Load() {
		err = ErrClosed
	} else {
		d.closeLock.Lock()
		defer d.closeLock.Unlock()
		d.close.Do(func() {
			err = d.db.Close()
		})
	}
	return
}

type leveldbBatch struct {
	b          *leveldb.Batch
	db         *leveldb.DB
	syncWrites bool
}

// Batch returns a new levelDB batcher
func (d *Datastore) Batch() (ds.Batch, error) {
	return &leveldbBatch{
		b:          new(leveldb.Batch),
		db:         d.db,
		syncWrites: d.syncWrites,
	}, nil
}

func (b *leveldbBatch) Put(key ds.Key, value []byte) error {
	b.b.Put(key.Bytes(), value)
	return nil
}

func (b *leveldbBatch) Commit() error {
	return b.db.Write(b.b, &opt.WriteOptions{Sync: b.syncWrites})
}

func (b *leveldbBatch) Delete(key ds.Key) error {
	b.b.Delete(key.Bytes())
	return nil
}

// A leveldb transaction embedding the accessor backed by the transaction.
type transaction struct {
	*Datastore
	tx *leveldb.Transaction
}

func (t *transaction) Commit() error {
	if t.closed.Load() {
		return ErrClosed
	}
	return t.tx.Commit()
}

func (t *transaction) Discard() {
	if t.closed.Load() {
		return
	}
	t.tx.Discard()
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
	return &transaction{Datastore: d, tx: tx}, nil
}
