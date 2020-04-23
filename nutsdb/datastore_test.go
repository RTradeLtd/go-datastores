package nutsdb

import (
	"os"
	"testing"

	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

var (
	testDir = "./tmp/nutsdb"
)

func TestDatastore(t *testing.T) {
	ds, err := New(testDir, DefaultOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
		os.RemoveAll(testDir)
		os.Remove(testDir)
	}()
	key := datastore.NewKey("hello/world")
	// ensure that tests which return datastore.ErrNotFound
	// behave as expected (they all should return that error)
	t.Run("NotFoundTests", func(t *testing.T) {
		if has, err := ds.Has(key); err != nil && err != datastore.ErrNotFound {
			t.Fatal("bad error returned: ", err)
		} else if has {
			t.Fatal("should not have")
		}
		if _, err := ds.GetSize(key); err != nil && err != datastore.ErrNotFound {
			t.Fatal("bad error returned: ", err)
		}
	})
	t.Run("ShouldPassTests", func(t *testing.T) {
		if err := ds.Put(key, []byte("hello/world")); err != nil {
			t.Fatal(err)
		}
		val, err := ds.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if string(val) != "hello/world" {
			t.Fatal("bad value returned")
		}
		has, err := ds.Has(key)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatal("should have key")
		}
		if err := ds.Sync(datastore.Key{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("BatchTests", func(t *testing.T) {
		batch, err := ds.Batch()
		if err != nil {
			t.Fatal(err)
		}
		key := datastore.NewKey("batchingkey")
		key2 := datastore.NewKey("shouldbedeleted")
		for _, k := range []datastore.Key{key, key2} {
			if err := batch.Put(key, []byte("this is a batch test")); err != nil {
				t.Fatal(err)
			}
			if err := batch.Delete(k); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Put(key, []byte("this is a batch test key again")); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(); err != nil {
			t.Fatal(err)
		}
		val, err := ds.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if string(val) != "this is a batch test key again" {
			t.Fatal("bad value returned")
		}
		_, err = ds.Get(key2)
		if err != datastore.ErrNotFound {
			t.Fatal("bad error returned")
		}
	})
}

func TestSuite(t *testing.T) {
	t.Skip("not yet implemented")
	ds, err := New(testDir, DefaultOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
		os.RemoveAll(testDir)
		os.Remove(testDir)
	}()
	dstest.SubtestAll(t, ds)
}
