package postgres

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"testing"

	sqlds "github.com/RTradeLtd/go-datastores/sql"
	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
	"/g":     "",
}

func newDS(t *testing.T) (*sqlds.Datastore, func(t *testing.T)) {
	opts := &Options{
		Host:           "127.0.0.1",
		Port:           "5432",
		User:           "postgres",
		Database:       "datastores",
		Password:       "password123",
		SSLMode:        "disable",
		RunMigrations:  true,
		RecreateTables: true,
	}
	ds, err := opts.Create()
	if err != nil {
		t.Fatal(err)
	}
	return ds, func(t *testing.T) {
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func addTestCases(t *testing.T, d *sqlds.Datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := datastore.NewKey(k)
		if err := d.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := datastore.NewKey(k)
		v2, err := d.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		v2b := v2
		if string(v2b) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}
}

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
	if queries.Delete() != queries.deleteQuery {
		t.Fatal("bad query returned")
	}
	if queries.Exists() != queries.existsQuery {
		t.Fatal("bad query returned")
	}
	if queries.Get() != queries.getQuery {
		t.Fatal("bad query returned")
	}
	if queries.Put() != queries.putQuery {
		t.Fatal("bad query returned")
	}
	if queries.Query() != queries.queryQuery {
		t.Fatal("bad query returned")
	}
	if queries.Prefix() != queries.prefixQuery {
		t.Fatal("bad query returned")
	}
	if queries.Limit() != queries.limitQuery {
		t.Fatal("bad query returned")
	}
	if queries.Offset() != queries.offsetQuery {
		t.Fatal("bad query returned")
	}
	if queries.GetSize() != queries.getSizeQuery {
		t.Fatal("bad query returned")
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

func TestQuery(t *testing.T) {
	d, done := newDS(t)
	defer done()

	addTestCases(t, d, testcases)

	// test prefix
	rs, err := d.Query(dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}
	expectMatches(t, []string{
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
	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)

	// test orders
	orbk := dsq.OrderByKey{}
	orderByKey := []dsq.Order{orbk}
	rs, err = d.Query(dsq.Query{Prefix: "/a/", Orders: orderByKey})
	if err != nil {
		t.Fatal(err)
	}
	expectKeyOrderMatches(t, rs, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	})

	orbkd := dsq.OrderByKeyDescending{}
	orderByDesc := []dsq.Order{orbkd}
	rs, err = d.Query(dsq.Query{Prefix: "/a/", Orders: orderByDesc})
	if err != nil {
		t.Fatal(err)
	}
	expectKeyOrderMatches(t, rs, []string{
		"/a/d",
		"/a/c",
		"/a/b/d",
		"/a/b/c",
		"/a/b",
	})

	// test filters
	equalFilter := dsq.FilterKeyCompare{Op: dsq.Equal, Key: "/a/b"}
	equalFilters := []dsq.Filter{equalFilter}
	rs, err = d.Query(dsq.Query{Prefix: "/a/", Filters: equalFilters})
	if err != nil {
		t.Fatal(err)
	}
	expectKeyFilterMatches(t, rs, []string{"/a/b"})

	greaterThanFilter := dsq.FilterKeyCompare{Op: dsq.GreaterThan, Key: "/a/b"}
	greaterThanFilters := []dsq.Filter{greaterThanFilter}
	rs, err = d.Query(dsq.Query{Prefix: "/a/", Filters: greaterThanFilters})
	if err != nil {
		t.Fatal(err)
	}
	expectKeyFilterMatches(t, rs, []string{
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	})

	lessThanFilter := dsq.FilterKeyCompare{Op: dsq.LessThanOrEqual, Key: "/a/b/c"}
	lessThanFilters := []dsq.Filter{lessThanFilter}
	rs, err = d.Query(dsq.Query{Prefix: "/a/", Filters: lessThanFilters})
	if err != nil {
		t.Fatal(err)
	}
	expectKeyFilterMatches(t, rs, []string{
		"/a/b",
		"/a/b/c",
	})
}

func TestHas(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	if !has {
		t.Error("Key should be found")
	}

	has, err = d.Has(ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}
}

func TestNotExistGet(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}

	val, err := d.Get(ds.NewKey("/a/b/c/d"))
	if val != nil {
		t.Error("Key should not be found")
	}

	if err != ds.ErrNotFound {
		t.Error("Error was not set to ds.ErrNotFound")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestDelete(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Error("Key should be found")
	}

	err = d.Delete(ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	has, err = d.Has(ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if has {
		t.Error("Key should not be found")
	}
}

func TestGetEmpty(t *testing.T) {
	d, done := newDS(t)
	defer done()

	err := d.Put(ds.NewKey("/a"), []byte{})
	if err != nil {
		t.Error(err)
	}

	v, err := d.Get(ds.NewKey("/a"))
	if err != nil {
		t.Error(err)
	}

	if len(v) != 0 {
		t.Error("expected 0 len []byte form get")
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()

	b, err := d.Batch()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}

	//Test delete
	b, err = d.Batch()
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(ds.NewKey("/a/b"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(ds.NewKey("/a/b/c"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rs, err := d.Query(dsq.Query{Prefix: "/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a",
		"/a/b/d",
		"/a/c",
		"/a/d",
		"/e",
		"/f",
		"/g",
	}, rs)
}

func SubtestBasicPutGet(t *testing.T) {
	d, done := newDS(t)
	defer done()

	k := ds.NewKey("foo")
	val := []byte("Hello Datastore!")

	err := d.Put(k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := d.Has(k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	size, err := d.GetSize(k)
	if err != nil {
		t.Fatal("error getting size after put: ", err)
	}
	if size != len(val) {
		t.Fatalf("incorrect size: expected %d, got %d", len(val), size)
	}

	out, err := d.Get(k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get wasnt what we expected:", out)
	}

	have, err = d.Has(k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	size, err = d.GetSize(k)
	if err != nil {
		t.Fatal("error getting size after get: ", err)
	}
	if size != len(val) {
		t.Fatalf("incorrect size: expected %d, got %d", len(val), size)
	}

	err = d.Delete(k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = d.Has(k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}

	size, err = d.GetSize(k)
	switch err {
	case ds.ErrNotFound:
	case nil:
		t.Fatal("expected error getting size after delete")
	default:
		t.Fatal("wrong error getting size after delete: ", err)
	}
	if size != -1 {
		t.Fatal("expected missing size to be -1")
	}
}

func TestNotFounds(t *testing.T) {
	d, done := newDS(t)
	defer done()

	badk := ds.NewKey("notreal")

	val, err := d.Get(badk)
	if err != ds.ErrNotFound {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := d.Has(badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}

	size, err := d.GetSize(badk)
	switch err {
	case ds.ErrNotFound:
	case nil:
		t.Fatal("expected error getting size after delete")
	default:
		t.Fatal("wrong error getting size after delete: ", err)
	}
	if size != -1 {
		t.Fatal("expected missing size to be -1")
	}
}

func SubtestManyKeysAndQuery(t *testing.T) {
	d, done := newDS(t)
	defer done()

	var keys []ds.Key
	var keystrs []string
	var values [][]byte
	count := 100
	for i := 0; i < count; i++ {
		s := fmt.Sprintf("%dkey%d", i, i)
		dsk := ds.NewKey(s)
		keystrs = append(keystrs, dsk.String())
		keys = append(keys, dsk)
		buf := make([]byte, 64)
		_, _ = rand.Read(buf)
		values = append(values, buf)
	}

	t.Logf("putting %d values", count)
	for i, k := range keys {
		err := d.Put(k, values[i])
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, k := range keys {
		val, err := d.Get(k)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		if !bytes.Equal(val, values[i]) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}

	t.Log("querying values")
	q := dsq.Query{KeysOnly: true}
	resp, err := d.Query(q)
	if err != nil {
		t.Fatal("calling query: ", err)
	}

	t.Log("aggregating query results")
	var outkeys []string
	for {
		res, ok := resp.NextSync()
		if res.Error != nil {
			t.Fatal("query result error: ", res.Error)
		}
		if !ok {
			break
		}

		outkeys = append(outkeys, res.Key)
	}

	t.Log("verifying query output")
	sort.Strings(keystrs)
	sort.Strings(outkeys)

	if len(keystrs) != len(outkeys) {
		t.Fatal("got wrong number of keys back")
	}

	for i, s := range keystrs {
		if outkeys[i] != s {
			t.Fatalf("in key output, got %s but expected %s", outkeys[i], s)
		}
	}

	t.Log("deleting all keys")
	for _, k := range keys {
		if err := d.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
}

// Tests from basic_tests from go-datastore
func TestBasicPutGet(t *testing.T) {
	d, done := newDS(t)
	defer done()

	k := ds.NewKey("foo")
	val := []byte("Hello Datastore!")

	err := d.Put(k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := d.Has(k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	out, err := d.Get(k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get wasnt what we expected:", out)
	}

	have, err = d.Has(k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	err = d.Delete(k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = d.Has(k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}
	SubtestBasicPutGet(t)
}

func TestManyKeysAndQuery(t *testing.T) {
	d, done := newDS(t)
	defer done()

	var keys []ds.Key
	var keystrs []string
	var values [][]byte
	count := 100
	for i := 0; i < count; i++ {
		s := fmt.Sprintf("%dkey%d", i, i)
		dsk := ds.NewKey(s)
		keystrs = append(keystrs, dsk.String())
		keys = append(keys, dsk)
		buf := make([]byte, 64)
		_, _ = rand.Read(buf)
		values = append(values, buf)
	}

	t.Logf("putting %d values", count)
	for i, k := range keys {
		err := d.Put(k, values[i])
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, k := range keys {
		val, err := d.Get(k)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		if !bytes.Equal(val, values[i]) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}

	t.Log("querying values")
	q := dsq.Query{KeysOnly: true}
	resp, err := d.Query(q)
	if err != nil {
		t.Fatal("calling query: ", err)
	}

	t.Log("aggregating query results")
	var outkeys []string
	for {
		res, ok := resp.NextSync()
		if res.Error != nil {
			t.Fatal("query result error: ", res.Error)
		}
		if !ok {
			break
		}

		outkeys = append(outkeys, res.Key)
	}

	t.Log("verifying query output")
	sort.Strings(keystrs)
	sort.Strings(outkeys)

	if len(keystrs) != len(outkeys) {
		t.Fatalf("got wrong number of keys back, %d != %d", len(keystrs), len(outkeys))
	}

	for i, s := range keystrs {
		if outkeys[i] != s {
			t.Fatalf("in key output, got %s but expected %s", outkeys[i], s)
		}
	}

	t.Log("deleting all keys")
	for _, k := range keys {
		if err := d.Delete(k); err != nil {
			t.Fatal(err)
		}
	}

	SubtestManyKeysAndQuery(t)
}

func TestTxn(t *testing.T) {
	d, done := newDS(t)
	defer done()
	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	testKey := ds.NewKey("helloworld")
	failKey := ds.NewKey("donothave")
	testValue := []byte("hello world")
	if err := txn.Put(testKey, testValue); err != nil {
		t.Fatal(err)
	}
	if err := txn.Delete(testKey); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(testKey, testValue); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Query(dsq.Query{}); err == nil {
		t.Fatal("error expected")
	}
	if size, err := txn.GetSize(testKey); err != nil {
		t.Fatal(err)
	} else if size != len(testValue) {
		t.Fatalf("bad size, got %v, wanted %v", size, len(testValue))
	}
	if has, err := txn.Has(testKey); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Fatal("should have key")
	}
	if has, err := txn.Has(failKey); err != nil {
		t.Fatal(err)
	} else if has {
		t.Fatal("should not have key")
	}
	if val, err := txn.Get(testKey); err != nil {
		t.Fatal(err)
	} else if string(val) != string(testValue) {
		t.Fatal("bad value returned")
	}
	if _, err := txn.Get(failKey); err != ds.ErrNotFound {
		t.Fatal("bad error returned")
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err == nil {
		t.Fatal("error expected")
	}
	txn.Discard()
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()

	dstest.SubtestAll(t, d)
}

func expectKeyFilterMatches(t *testing.T, actual dsq.Results, expect []string) {
	t.Helper()
	actualE, err := actual.Rest()
	if err != nil {
		t.Error(err)
		return
	}
	actualS := make([]string, len(actualE))
	for i, e := range actualE {
		actualS[i] = e.Key
	}

	if len(actualS) != len(expect) {
		t.Error("length doesn't match.", expect, actualS)
		return
	}

	if strings.Join(actualS, "") != strings.Join(expect, "") {
		t.Error("expect != actual.", expect, actualS)
		return
	}
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	t.Helper()
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func expectKeyOrderMatches(t *testing.T, actual dsq.Results, expect []string) {
	t.Helper()
	rs, err := actual.Rest()
	if err != nil {
		t.Error("error fetching dsq.Results", expect, actual)
		return
	}

	if len(rs) != len(expect) {
		t.Error("expect != actual.", expect, actual)
		return
	}

	for i, r := range rs {
		if r.Key != expect[i] {
			t.Error("expect != actual.", expect, actual)
			return
		}
	}
}
