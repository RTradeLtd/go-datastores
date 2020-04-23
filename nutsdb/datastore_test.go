package nutsdb

import (
	"os"
	"testing"

	"github.com/ipfs/go-datastore"
)

var (
	testDir = "./tmp/nutsdb"
)

func TestDatastore(t *testing.T) {
	ds, err := New(testDir, DefaultOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
		os.RemoveAll(testDir)
	})
	if err := ds.Put(datastore.NewKey("hello/world"), []byte("hello/world")); err != nil {
		t.Fatal(err)
	}
	val, err := ds.Get(datastore.NewKey("hello/world"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "hello/world" {
		t.Fatal("bad value retrieved")
	}
}
