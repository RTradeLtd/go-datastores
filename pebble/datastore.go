package dspebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

var (
	_ datastore.Batching            = (*Datastore)(nil)
	_ datastore.PersistentDatastore = (*Datastore)(nil)
)

// Datastore implements a pebble backed ipfs datastore
type Datastore struct {
	db       *pebble.DB
	walStats bool
	withSync bool
}

// NewDatastore instantiates a new pebble datastore
func NewDatastore(path string, opts *pebble.Options, withSync bool) (*Datastore, error) {
	ds, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &Datastore{ds, false, withSync}, nil
}

// Put is used to store a value named by key
func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Set(key.Bytes(), value, &pebble.WriteOptions{Sync: d.withSync})
}

// Delete removes the value for given `key`.
func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Delete(key.Bytes(), &pebble.WriteOptions{Sync: d.withSync})
}

// Get is used to return a value named key from our datastore
func (d *Datastore) Get(key datastore.Key) ([]byte, error) {
	data, closer, err := d.db.Get(key.Bytes())
	if err != nil {
		return nil, datastore.ErrNotFound
	}
	copied := append(data[:0:0], data...)
	return copied, closer.Close()
}

// Has is used to check if we have a value named key in our datastore
func (d *Datastore) Has(key datastore.Key) (bool, error) {
	return datastore.GetBackedHas(d, key)
}

// GetSize is used to get the size of a value named key
func (d *Datastore) GetSize(key datastore.Key) (int, error) {
	return datastore.GetBackedSize(d, key)
}

// Query is used to search a datastore for keys, and optionally values
// matching a given query
func (d *Datastore) Query(q query.Query) (query.Results, error) {

	var (
		//entries []query.Entry
		snap = d.db.NewSnapshot()
		iter = snap.NewIter(&pebble.IterOptions{
			LowerBound: func() []byte {
				prefix := datastore.NewKey(q.Prefix).String()
				if prefix != "/" {
					return []byte(prefix)
				}
				return nil
			}(),
		})
	)
	qNaive := q
	next := iter.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case query.OrderByKey, *query.OrderByKey:
			qNaive.Orders = nil
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			next = func() bool {
				next = iter.Prev
				return iter.Last()
			}
			qNaive.Orders = nil
		default:
		}
	} //defer iter.Close()
	defer snap.Close()
	r := query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			if !next() {
				return query.Result{}, false
			}
			entry := query.Entry{
				Key:  string(iter.Key()),
				Size: len(iter.Value()),
			}
			if !q.KeysOnly {
				entry.Value = append(iter.Value()[0:0:0], iter.Value()...)
			}
			return query.Result{Entry: entry}, false
		},
		Close: func() error {
			return iter.Close()
		},
	})
	return query.NaiveQueryApply(qNaive, r), nil
}

// Sync is used to manually trigger syncing db contents to disk.
// This call is only usable when synchronous writes aren't enabled
func (d *Datastore) Sync(prefix datastore.Key) error {
	if d.withSync {
		return nil
	}
	return d.db.Flush()
}

type pebbleBatch struct {
	db *pebble.Batch
}

// Batch returns a batchable datastore useful for combining
// many operations into one
func (d *Datastore) Batch() (datastore.Batch, error) {
	return &pebbleBatch{
		db: d.db.NewBatch(),
	}, nil
}

// Put is used to load a Put operation as a batch based op
func (b *pebbleBatch) Put(key datastore.Key, value []byte) error {
	return b.db.Set(key.Bytes(), value, &pebble.WriteOptions{Sync: true})
}

// Delete is used to load a Delete operation as a batch based op
func (b *pebbleBatch) Delete(key datastore.Key) error {
	return b.db.Delete(key.Bytes(), &pebble.WriteOptions{Sync: true})
}

// Commit is used to commit all operation
func (b *pebbleBatch) Commit() error {
	return b.db.Commit(&pebble.WriteOptions{Sync: true})
}

// DiskUsage returns the space used by our datastore in bytes
// it does not include the WAL (Write Ahead Log) size
// and only includes total size from all the "levels"
func (d *Datastore) DiskUsage() (uint64, error) {
	var totalSize uint64
	for _, level := range d.db.Metrics().Levels {
		totalSize = totalSize + level.Size
	}
	if d.walStats {
		totalSize = totalSize + d.db.Metrics().WAL.Size
	}
	return totalSize, nil
}

// ToggleWALStats is used to toggle reporting of
// WAL statistics when runnning DiskUsage
func (d *Datastore) ToggleWALStats() {
	d.walStats = !d.walStats
}

// Close is used to terminate our datastore connection
func (d *Datastore) Close() error {
	return d.db.Close()
}
