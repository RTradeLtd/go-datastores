package leveldb

import (
	ds "github.com/ipfs/go-datastore"

	"github.com/ucwong/goleveldb/leveldb"
	"github.com/ucwong/goleveldb/leveldb/opt"
)

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
