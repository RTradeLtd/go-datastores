package leveldb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/RTradeLtd/go-datastores/testutils"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/ucwong/goleveldb/leveldb"
)

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//  d, close := newDS(t)
//  defer close()
func newDS(t *testing.T) (*Datastore, func()) {
	path := "testpath"
	opts := Options{}
	opts.NoSync = true
	d, err := NewDatastore(path, &opts)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		d.Close()
		os.RemoveAll(path)
	}
}

func testQuery(t *testing.T, d *Datastore) {
	testutils.AddTestCases(t, d, testutils.TestCases)

	rs, err := d.Query(dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	testutils.ExpectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = d.Query(dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	testutils.ExpectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)

	// test order

	rs, err = d.Query(dsq.Query{Orders: []dsq.Order{dsq.OrderByKey{}}})
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]string, 0, len(testutils.TestCases))
	for k := range testutils.TestCases {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	testutils.ExpectKeyOrderMatches(t, rs, keys)

	rs, err = d.Query(dsq.Query{Orders: []dsq.Order{dsq.OrderByKeyDescending{}}})
	if err != nil {
		t.Fatal(err)
	}

	// reverse
	for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
		keys[i], keys[j] = keys[j], keys[i]
	}

	testutils.ExpectKeyOrderMatches(t, rs, keys)
}

func TestEmptyOpts(t *testing.T) {
	path := "emptyoptstest"
	ds, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.Close(); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(path)
}

func TestQuery(t *testing.T) {
	d, close := newDS(t)
	defer close()
	testQuery(t, d)
}

func TestQueryRespectsProcess(t *testing.T) {
	d, close := newDS(t)
	defer close()
	testutils.AddTestCases(t, d, testutils.TestCases)
}

func TestCloseRace(t *testing.T) {
	d, close := newDS(t)
	for n := 0; n < 100; n++ {
		d.Put(ds.NewKey(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("test%d", n)))
	}
	tx, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	tx.Put(ds.NewKey("txnversion"), []byte("bump"))
	closeCh := make(chan interface{})

	go func() {
		close()
		closeCh <- nil
	}()
	for k := range testutils.TestCases {
		tx.Get(ds.NewKey(k))
	}
	tx.Commit()
	<-closeCh
}

func TestCloseSafety(t *testing.T) {
	d, close := newDS(t)
	testutils.AddTestCases(t, d, testutils.TestCases)

	tx, _ := d.NewTransaction(false)
	err := tx.Put(ds.NewKey("test"), []byte("test"))
	if err != nil {
		t.Error("Failed to put in a txn.")
	}
	close()
	err = tx.Commit()
	if err == nil {
		t.Error("committing after close should fail.")
	}
	if err := d.Close(); err != ErrClosed {
		t.Fatal("bad error returned, got: ", err)
	}
}

func testBatching(t *testing.T, d *Datastore) {
	b, err := d.Batch()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testutils.TestCases {
		err := b.Put(ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testutils.TestCases {
		val, err := d.Get(ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()
	testBatching(t, d)
}

func TestDiskUsage(t *testing.T) {
	d, done := newDS(t)
	testutils.AddTestCases(t, d, testutils.TestCases)
	du, err := d.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}

	if du == 0 {
		t.Fatal("expected some disk usage")
	}

	k := ds.NewKey("more")
	err = d.Put(k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	du2, err := d.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	if du2 <= du {
		t.Fatal("size should have increased")
	}

	done()

	// This should fail
	_, err = d.DiskUsage()
	if err == nil {
		t.Fatal("DiskUsage should fail when we cannot walk path")
	}
}

func TestTransactionCommit(t *testing.T) {
	key := ds.NewKey("/test/key1")

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()

	if err := txn.Put(key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != nil || !bytes.Equal(val, []byte("hello")) {
		t.Fatalf("expected entry present after commit, got err: %v, value: %v", err, val)
	}
}

func TestTransactionDiscard(t *testing.T) {
	key := ds.NewKey("/test/key1")

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()

	if err := txn.Put(key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if txn.Discard(); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
}

func TestTransactionManyOperations(t *testing.T) {
	keys := []ds.Key{ds.NewKey("/test/key1"), ds.NewKey("/test/key2"), ds.NewKey("/test/key3"), ds.NewKey("/test/key4"), ds.NewKey("/test/key5")}

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()

	// Insert all entries.
	for i := 0; i < 5; i++ {
		if err := txn.Put(keys[i], []byte(fmt.Sprintf("hello%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Remove the third entry.
	if err := txn.Delete(keys[2]); err != nil {
		t.Fatal(err)
	}

	// Check existences.
	if has, err := txn.Has(keys[1]); err != nil || !has {
		t.Fatalf("expected key[1] to be present, err: %v, has: %v", err, has)
	}
	if has, err := txn.Has(keys[2]); err != nil || has {
		t.Fatalf("expected key[2] to be absent, err: %v, has: %v", err, has)
	}

	var res dsq.Results
	if res, err = txn.Query(dsq.Query{Prefix: "/test"}); err != nil {
		t.Fatalf("query failed, err: %v", err)
	}
	if entries, err := res.Rest(); err != nil || len(entries) != 4 {
		t.Fatalf("query failed or contained unexpected number of entries, err: %v, results: %v", err, entries)
	}

	txn.Discard()
}

func TestHandleGetErr(t *testing.T) {
	if err := handleGetError(nil); err != nil {
		t.Fatal(err)
	}
	if err := handleGetError(
		leveldb.ErrNotFound,
	); err != ds.ErrNotFound {
		t.Fatal("bad error")
	}
	if err := handleGetError(
		errors.New("misc err"),
	); err == nil {
		t.Fatal("error expected")
	} else if err.Error() != "misc err" {
		t.Fatal("bad error returned")
	}
}

func TestSuite(t *testing.T) {
	d, close := newDS(t)
	defer close()
	dstest.SubtestAll(t, d)
}
