package nutsdb

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/xujiajun/nutsdb"
)

var (
	_ datastore.Batching = (*Datastore)(nil)
	// DefaultOpts exposes the default nutsdb options
	DefaultOpts = nutsdb.DefaultOptions
	bucketName  = "nutsdb-bucket"
)

// Datastore provides a key-value store using nutsdb
type Datastore struct {
	db *nutsdb.DB
}

// New returns a datastore backed by nutsdb
func New(dir string, opts nutsdb.Options) (*Datastore, error) {
	opts.Dir = dir
	db, err := nutsdb.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Datastore{db}, nil
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (d *Datastore) Get(key datastore.Key) ([]byte, error) {
	var data []byte
	if err := d.db.View(func(tx *nutsdb.Tx) error {
		entry, err := tx.Get(bucketName, key.Bytes())
		if err != nil {
			return err
		}
		data = entry.Value
		return nil
	}); err != nil {
		if strings.Contains(err.Error(), "not found") {
			err = datastore.ErrNotFound
		}
		return nil, err
	}
	if data == nil {
		return nil, datastore.ErrNotFound
	}
	return data, nil
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
// The default implementation is found in `GetBackedHas`.
func (d *Datastore) Has(key datastore.Key) (exists bool, err error) {
	return datastore.GetBackedHas(d, key)
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (d *Datastore) GetSize(key datastore.Key) (size int, err error) {
	return datastore.GetBackedSize(d, key)
}

// Put stores the object `value` named by `key`.
//
// The generalized Datastore interface does not impose a value type,
// allowing various datastore middleware implementations (which do not
// handle the values directly) to be composed together.
//
// Ultimately, the lowest-level datastore will need to do some value checking
// or risk getting incorrect values. It may also be useful to expose a more
// type-safe interface to your application, and do the checking up-front.
func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucketName, key.Bytes(), value, nutsdb.Persistent)
	})
}

// Batch enables batching multiple operations together to reduce disk I/O
func (d *Datastore) Batch() (datastore.Batch, error) {
	tx, err := d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &nutBatcher{d: d, tx: tx}, nil
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(bucketName, key.Bytes())
	})
}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//   result, _ := ds.Query(q)
//
//   // use the channel interface; result may come in at different times
//   for entry := range result.Next() { ... }
//
//   // or wait for the query to be completely done
//   entries, _ := result.Rest()
//   for entry := range entries { ... }
//
func (d *Datastore) Query(q query.Query) (query.Results, error) {
	//resb := query.NewResultBuilder(q)
	var (
		results   []query.Entry
		prefix    []byte
		prefixKey = datastore.NewKey(q.Prefix).String()
	)
	if prefixKey != "/" {
		prefix = []byte(prefixKey + "/")
	}
	if err := d.db.View(func(tx *nutsdb.Tx) (err error) {
		var entries nutsdb.Entries
		if prefix != nil {
			entries, _, err = tx.PrefixScan(bucketName, prefix, q.Offset, q.Limit)
			if err != nil {
				return err
			}
		} else {
			entries, _, err = tx.PrefixSearchScan(
				bucketName, prefix, string(prefix), q.Offset, q.Limit)
		}
		results = make([]query.Entry, 0, len(entries))
		for _, entry := range entries {
			qen := query.Entry{
				Key:  string(entry.Key),
				Size: int(entry.Size()),
			}
			if filter(q.Filters, qen) {
				continue
			}
			if !q.KeysOnly {
				qen.Value = entry.Value
			}
			results = append(results, qen)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	queryResults := query.ResultsReplaceQuery(query.ResultsWithEntries(q, results), q)
	return query.NaiveQueryApply(q, queryResults), nil
}

// Sync guarantees that any Put or Delete calls under prefix that returned
// before Sync(prefix) was called will be observed after Sync(prefix)
// returns, even if the program crashes. If Put/Delete operations already
// satisfy these requirements then Sync may be a no-op.
//
// If the prefix fails to Sync this method returns an error.
func (d *Datastore) Sync(prefix datastore.Key) error {
	return d.db.ActiveFile.Sync()
}

// Close shutsdown the datastore
func (d *Datastore) Close() error {
	serr := d.Sync(datastore.Key{})
	if err := d.db.Close(); err != nil {
		serr = fmt.Errorf(
			"closure error: %s, sync error: %s",
			err, serr,
		)
	}
	return serr
}

// filter returns _true_ if we should filter (skip) the entry
func filter(filters []query.Filter, entry query.Entry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}
