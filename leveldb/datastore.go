package leveldb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/ucwong/goleveldb/leveldb"
	"github.com/ucwong/goleveldb/leveldb/opt"
	"github.com/ucwong/goleveldb/leveldb/storage"
	"github.com/ucwong/goleveldb/leveldb/util"
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
	closeLock  sync.RWMutex
}

// Options is an alias of syndtr/goleveldb/opt.Options which might be extended
// in the future.
type Options = opt.Options

// NewDatastore returns a new datastore backed by leveldb
//
// for path == "", an in memory backend will be chosen
func NewDatastore(path string, opts *Options) (*Datastore, error) {
	var noSync bool
	if opts != nil {
		noSync = opts.NoSync
	}

	var err error
	var db *leveldb.DB

	if path == "" {
		db, err = leveldb.Open(storage.NewMemStorage(), opts)
	} else {
		db, err = leveldb.OpenFile(path, opts)
	}
	if err != nil {
		return nil, err
	}

	ds := Datastore{
		db:     db,
		path:   path,
		closed: atomic.NewBool(false),
		// if noSync is false this will be true
		syncWrites: !noSync,
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
		return nil, handleGetError(err)
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
	var rnge *util.Range

	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		rnge = util.BytesPrefix([]byte(prefix + "/"))
		qNaive.Prefix = ""
	}
	iter := d.db.NewIterator(rnge, nil)
	res, err := query(iter, q, qNaive)
	d.closeLock.Unlock()
	return res, err
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
	err = ErrClosed
	d.close.Do(func() {
		d.closeLock.Lock()
		err = d.db.Close()
		d.closed.Store(true)
		d.closeLock.Unlock()
	})
	return
}
