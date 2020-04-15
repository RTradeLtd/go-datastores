package badger

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/tevino/abool"
)

// ErrClosed is an error message returned when the datastore is no longer open
var ErrClosed = errors.New("datastore closed")

// Datastore satisfies the Datastore::Batching interface using badger
type Datastore struct {
	db *badger.DB

	closed  *abool.AtomicBool
	closing chan struct{}

	gcDiscardRatio float64
	gcSleep        time.Duration
	gcInterval     time.Duration

	syncWrites bool
}

// Implements the datastore.Txn interface, enabling transaction support for
// the badger Datastore.
type txn struct {
	ds  *Datastore
	txn *badger.Txn
	// Whether this transaction has been implicitly created as a result of a direct Datastore
	// method invocation.
	implicit bool
}

// Options are the badger datastore options, reexported here for convenience.
type Options struct {
	// Please refer to the Badger docs to see what this is for
	GcDiscardRatio float64

	// Interval between GC cycles
	//
	// If zero, the datastore will perform no automatic garbage collection.
	GcInterval time.Duration

	// Sleep time between rounds of a single GC cycle.
	//
	// If zero, the datastore will only perform one round of GC per
	// GcInterval.
	GcSleep time.Duration

	badger.Options
}

// DefaultOptions are the default options for the badger datastore.
var DefaultOptions Options

func init() {
	DefaultOptions = Options{
		GcDiscardRatio: 0.2,
		GcInterval:     15 * time.Minute,
		GcSleep:        10 * time.Second,
		Options:        badger.DefaultOptions(""),
	}
	DefaultOptions.Options.CompactL0OnClose = false
	DefaultOptions.Options.Truncate = true
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)
var _ ds.TTLDatastore = (*Datastore)(nil)
var _ ds.GCDatastore = (*Datastore)(nil)

// NewDatastore creates a new badger datastore.
//
// DO NOT set the Dir and/or ValuePath fields of opt, they will be set for you.
func NewDatastore(path string, options *Options) (*Datastore, error) {
	// Copy the options because we modify them.
	var opt badger.Options
	var gcDiscardRatio float64
	var gcSleep time.Duration
	var gcInterval time.Duration
	if options == nil {
		opt = badger.DefaultOptions("")
		gcDiscardRatio = DefaultOptions.GcDiscardRatio
		gcSleep = DefaultOptions.GcSleep
		gcInterval = DefaultOptions.GcInterval
	} else {
		opt = options.Options
		gcDiscardRatio = options.GcDiscardRatio
		gcSleep = options.GcSleep
		gcInterval = options.GcInterval
	}

	if gcSleep <= 0 {
		// If gcSleep is 0, we don't perform multiple rounds of GC per
		// cycle.
		gcSleep = gcInterval
	}

	opt.Dir = path
	opt.ValueDir = path

	kv, err := badger.Open(opt)
	if err != nil {
		if strings.HasPrefix(err.Error(), "manifest has unsupported version:") {
			err = fmt.Errorf("unsupported badger version, use github.com/ipfs/badgerds-upgrade to upgrade: %s", err.Error())
		}
		return nil, err
	}

	ds := &Datastore{
		db:             kv,
		closed:         abool.New(),
		closing:        make(chan struct{}),
		gcDiscardRatio: gcDiscardRatio,
		gcSleep:        gcSleep,
		gcInterval:     gcInterval,
		syncWrites:     opt.SyncWrites,
	}

	// Start the GC process if requested.
	if ds.gcInterval > 0 {
		go ds.periodicGC()
	}

	return ds, nil
}

// Keep scheduling GC's AFTER `gcInterval` has passed since the previous GC
func (d *Datastore) periodicGC() {
	gcTimeout := time.NewTimer(d.gcInterval)
	defer gcTimeout.Stop()

	for {
		select {
		case <-gcTimeout.C:
			switch err := d.gcOnce(); err {
			case badger.ErrNoRewrite, badger.ErrRejected:
				// No rewrite means we've fully garbage collected.
				// Rejected means someone else is running a GC
				// or we're closing.
				gcTimeout.Reset(d.gcInterval)
			case nil:
				gcTimeout.Reset(d.gcSleep)
			case ErrClosed:
				return
			default:
				log.Println("gc cycle error: ", err)
				// Not much we can do on a random error but log it and continue.
				gcTimeout.Reset(d.gcInterval)
			}
		case <-d.closing:
			return
		}
	}
}

// NewTransaction starts a new transaction. The resulting transaction object
// can be mutated without incurring changes to the underlying Datastore until
// the transaction is Committed.
func (d *Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	if d.closed.IsSet() {
		return nil, ErrClosed
	}
	return &txn{d, d.db.NewTransaction(!readOnly), false}, nil
}

// newImplicitTransaction creates a transaction marked as 'implicit'.
// Implicit transactions are created by Datastore methods performing single operations.
func (d *Datastore) newImplicitTransaction(readOnly bool) *txn {
	return &txn{d, d.db.NewTransaction(!readOnly), true}
}

// Put stores the value under the given key
func (d *Datastore) Put(key ds.Key, value []byte) error {
	if d.closed.IsSet() {
		return ErrClosed
	}
	txn := d.newImplicitTransaction(false)
	defer txn.discard()

	if err := txn.put(key, value); err != nil {
		return err
	}

	return txn.commit()
}

// Sync is used to manually trigger syncing db contents to disk.
// This call is only usable when synchronous writes aren't enabled
func (d *Datastore) Sync(prefix ds.Key) error {
	if d.closed.IsSet() {
		return ErrClosed
	}
	if d.syncWrites {
		return nil
	}

	return d.db.Sync()
}

// PutWithTTL puts the value udner the given key for the specific duration before being GC'd
func (d *Datastore) PutWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	if d.closed.IsSet() {
		return ErrClosed
	}
	txn := d.newImplicitTransaction(false)
	defer txn.discard()

	if err := txn.putWithTTL(key, value, ttl); err != nil {
		return err
	}

	return txn.commit()
}

// SetTTL is used to override the stored ttl for the given key
func (d *Datastore) SetTTL(key ds.Key, ttl time.Duration) error {
	if d.closed.IsSet() {
		return ErrClosed
	}

	txn := d.newImplicitTransaction(false)
	defer txn.discard()

	if err := txn.setTTL(key, ttl); err != nil {
		return err
	}

	return txn.commit()
}

// GetExpiration is used to get the ttl expiration time for the key
func (d *Datastore) GetExpiration(key ds.Key) (time.Time, error) {
	if d.closed.IsSet() {
		return time.Time{}, ErrClosed
	}

	txn := d.newImplicitTransaction(false)
	defer txn.discard()

	return txn.getExpiration(key)
}

// Get returns the value associated with the key
func (d *Datastore) Get(key ds.Key) (value []byte, err error) {
	if d.closed.IsSet() {
		return nil, ErrClosed
	}

	txn := d.newImplicitTransaction(true)
	defer txn.discard()

	return txn.get(key)
}

// Has returns whether or not we have the given key in our datastore
func (d *Datastore) Has(key ds.Key) (bool, error) {
	if d.closed.IsSet() {
		return false, ErrClosed
	}

	txn := d.newImplicitTransaction(true)
	defer txn.discard()

	return txn.has(key)
}

// GetSize returns the size of value associated with the key
func (d *Datastore) GetSize(key ds.Key) (size int, err error) {
	if d.closed.IsSet() {
		return -1, ErrClosed
	}

	txn := d.newImplicitTransaction(true)
	defer txn.discard()

	return txn.getSize(key)
}

// Delete remove the key+value from our datastore
func (d *Datastore) Delete(key ds.Key) error {
	if d.closed.IsSet() {
		return ErrClosed
	}
	txn := d.newImplicitTransaction(false)
	defer txn.discard()
	err := txn.delete(key)
	if err != nil {
		return err
	}
	return txn.commit()
}

// Query is used to perform a search of the keys and values in our datastore
func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	if d.closed.IsSet() {
		return nil, ErrClosed
	}
	txn := d.newImplicitTransaction(true)
	// We cannot defer txn.Discard() here, as the txn must remain active while the iterator is open.
	// https://github.com/dgraph-io/badger/commit/b1ad1e93e483bbfef123793ceedc9a7e34b09f79
	// The closing logic in the query goprocess takes care of discarding the implicit transaction.
	return txn.query(q)
}

// DiskUsage implements the PersistentDatastore interface.
// It returns the sum of lsm and value log files sizes in bytes.
func (d *Datastore) DiskUsage() (uint64, error) {
	if d.closed.IsSet() {
		return 0, ErrClosed
	}
	lsm, vlog := d.db.Size()
	return uint64(lsm + vlog), nil
}

// Close is used to close our datastore and cease operations.
func (d *Datastore) Close() error {
	if !d.closed.SetToIf(false, true) {
		return ErrClosed
	}
	close(d.closing)
	return d.db.Close()
}

// Batch is used to return a set of batchable transaction operatiosn
func (d *Datastore) Batch() (ds.Batch, error) {
	tx, _ := d.NewTransaction(false)
	return tx, nil
}

// CollectGarbage removes garbage from our underlying datastore
func (d *Datastore) CollectGarbage() (err error) {
	// The idea is to keep calling DB.RunValueLogGC() till Badger no longer has any log files
	// to GC(which would be indicated by an error, please refer to Badger GC docs).
	for err == nil {
		err = d.gcOnce()
	}

	if err == badger.ErrNoRewrite {
		err = nil
	}

	return err
}

func (d *Datastore) gcOnce() error {
	if d.closed.IsSet() {
		return ErrClosed
	}
	return d.db.RunValueLogGC(d.gcDiscardRatio)
}

var _ ds.Datastore = (*txn)(nil)
var _ ds.TTLDatastore = (*txn)(nil)

func (t *txn) Put(key ds.Key, value []byte) error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return t.put(key, value)
}

func (t *txn) put(key ds.Key, value []byte) error {
	return t.txn.Set(key.Bytes(), value)
}

func (t *txn) Sync(prefix ds.Key) error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return nil
}

func (t *txn) PutWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return t.putWithTTL(key, value, ttl)
}

func (t *txn) putWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	return t.txn.SetEntry(badger.NewEntry(key.Bytes(), value).WithTTL(ttl))
}

func (t *txn) GetExpiration(key ds.Key) (time.Time, error) {
	if t.ds.closed.IsSet() {
		return time.Time{}, ErrClosed
	}
	return t.getExpiration(key)
}

func (t *txn) getExpiration(key ds.Key) (time.Time, error) {
	item, err := t.txn.Get(key.Bytes())
	if err == badger.ErrKeyNotFound {
		return time.Time{}, ds.ErrNotFound
	} else if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(item.ExpiresAt()), 0), nil
}

func (t *txn) SetTTL(key ds.Key, ttl time.Duration) error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return t.setTTL(key, ttl)
}

func (t *txn) setTTL(key ds.Key, ttl time.Duration) error {
	item, err := t.txn.Get(key.Bytes())
	if err != nil {
		return err
	}
	return item.Value(func(data []byte) error {
		return t.putWithTTL(key, data, ttl)
	})

}

func (t *txn) Get(key ds.Key) ([]byte, error) {
	if t.ds.closed.IsSet() {
		return nil, ErrClosed
	}
	return t.get(key)
}

func (t *txn) get(key ds.Key) ([]byte, error) {
	item, err := t.txn.Get(key.Bytes())
	if err == badger.ErrKeyNotFound {
		err = ds.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return item.ValueCopy(nil)
}

func (t *txn) Has(key ds.Key) (bool, error) {
	if t.ds.closed.IsSet() {
		return false, ErrClosed
	}
	return t.has(key)
}

func (t *txn) has(key ds.Key) (bool, error) {
	_, err := t.txn.Get(key.Bytes())
	switch err {
	case badger.ErrKeyNotFound:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}

func (t *txn) GetSize(key ds.Key) (int, error) {
	if t.ds.closed.IsSet() {
		return -1, ErrClosed
	}
	return t.getSize(key)
}

func (t *txn) getSize(key ds.Key) (int, error) {
	item, err := t.txn.Get(key.Bytes())
	switch err {
	case nil:
		return int(item.ValueSize()), nil
	case badger.ErrKeyNotFound:
		return -1, ds.ErrNotFound
	default:
		return -1, err
	}
}

func (t *txn) Delete(key ds.Key) error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return t.delete(key)
}

func (t *txn) delete(key ds.Key) error {
	return t.txn.Delete(key.Bytes())
}

func (t *txn) Query(q dsq.Query) (dsq.Results, error) {
	if t.ds.closed.IsSet() {
		return nil, ErrClosed
	}
	return t.query(q)
}

func (t *txn) query(q dsq.Query) (dsq.Results, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = !q.KeysOnly
	opt.Prefix = []byte(q.Prefix)

	// Handle ordering
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
		// We order by key by default.
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			// Reverse order by key
			opt.Reverse = true
		default:
			// Ok, we have a weird order we can't handle. Let's
			// perform the _base_ query (prefix, filter, etc.), then
			// handle sort/offset/limit later.

			// Skip the stuff we can't apply.
			baseQuery := q
			baseQuery.Limit = 0
			baseQuery.Offset = 0
			baseQuery.Orders = nil

			// perform the base query.
			res, err := t.query(baseQuery)
			if err != nil {
				return nil, err
			}

			// fix the query
			res = dsq.ResultsReplaceQuery(res, q)

			// Remove the parts we've already applied.
			naiveQuery := q
			naiveQuery.Prefix = ""
			naiveQuery.Filters = nil

			// Apply the rest of the query
			return dsq.NaiveQueryApply(naiveQuery, res), nil
		}
	}
	var (
		it         = t.txn.NewIterator(opt)
		done       = make(chan bool)
		resultChan = make(chan dsq.Result)
		entries    = make([]dsq.Entry, 0)
	)
	go func() {
		defer func() {
			done <- true
		}()
		if t.ds.closed.IsSet() {
			return
		}
		// this iterator is part of an implicit transaction, so when
		// we're done we must discard the transaction. It's safe to
		// discard the txn it because it contains the iterator only.
		if t.implicit {
			defer t.discard()
		}
		defer it.Close()
		// All iterators must be started by rewinding.
		it.Rewind()
		// skip to the offset
		for skipped := 0; skipped < q.Offset && it.Valid(); it.Next() {
			// On the happy path, we have no filters and we can go
			// on our way.
			if len(q.Filters) == 0 {
				skipped++
				continue
			}
			// On the sad path, we need to apply filters before
			// counting the item as "skipped" as the offset comes
			// _after_ the filter.
			item := it.Item()
			matches := true
			check := func(value []byte) error {
				e := dsq.Entry{Key: string(item.Key()), Value: value, Size: int(item.ValueSize())}
				// Only calculate expirations if we need them.
				if q.ReturnExpirations {
					e.Expiration = expires(item)
				}
				matches = filter(q.Filters, e)
				return nil
			}
			// Maybe check with the value, only if we need it.
			var err error
			if q.KeysOnly {
				err = check(nil)
			} else {
				err = item.Value(check)
			}
			if err != nil {
				select {
				case resultChan <- dsq.Result{Error: err}:
				case <-t.ds.closing:
					return
				case <-ctx.Done():
					return
				}
			}
			if !matches {
				skipped++
			}
		}
		for sent := 0; (q.Limit <= 0 || sent < q.Limit) && it.Valid(); it.Next() {
			item := it.Item()
			e := dsq.Entry{Key: string(item.Key())}
			// Maybe get the value
			var result dsq.Result
			if !q.KeysOnly {
				b, err := item.ValueCopy(nil)
				if err != nil {
					result = dsq.Result{Error: err}
				} else {
					e.Value = b
					e.Size = len(b)
					result = dsq.Result{Entry: e}
				}
			} else {
				e.Size = int(item.ValueSize())
				result = dsq.Result{Entry: e}
			}

			if q.ReturnExpirations {
				result.Expiration = expires(item)
			}

			// Finally, filter it (unless we're dealing with an error).
			if result.Error == nil && filter(q.Filters, e) {
				continue
			}

			select {
			case resultChan <- result:
				sent++
			case <-ctx.Done():
				return
			case <-t.ds.closing:
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			goto FINISHED
		case result := <-resultChan:
			if result.Error != nil {
				log.Println("query result failure: ", result.Error)
			}
			entries = append(entries, result.Entry)
		}
	}
FINISHED:
	return dsq.ResultsWithEntries(q, entries), nil
}

func (t *txn) Commit() error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return t.commit()
}

func (t *txn) commit() error {
	return t.txn.Commit()
}

// Alias to commit
func (t *txn) Close() error {
	if t.ds.closed.IsSet() {
		return ErrClosed
	}
	return t.close()
}

func (t *txn) close() error {
	return t.txn.Commit()
}

func (t *txn) Discard() {
	if t.ds.closed.IsSet() {
		return
	}
	t.discard()
}

func (t *txn) discard() {
	t.txn.Discard()
}

// filter returns _true_ if we should filter (skip) the entry
func filter(filters []dsq.Filter, entry dsq.Entry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}

func expires(item *badger.Item) time.Time {
	return time.Unix(int64(item.ExpiresAt()), 0)
}
