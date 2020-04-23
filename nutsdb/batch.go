package nutsdb

import (
	"github.com/ipfs/go-datastore"
	"github.com/xujiajun/nutsdb"
)

// nutBatcher is an implement of the nuts datastore
// which provides batching capabilities
type nutBatcher struct {
	d  *Datastore
	tx *nutsdb.Tx
}

// Put is a batchable Put
func (ndb *nutBatcher) Put(key datastore.Key, value []byte) error {
	return ndb.tx.Put(bucketName, key.Bytes(), value, nutsdb.Persistent)
}

// Delete is a batchable Delete
func (ndb *nutBatcher) Delete(key datastore.Key) error {
	return ndb.tx.Delete(bucketName, key.Bytes())
}

// Commit flushes all updates to disk
func (ndb *nutBatcher) Commit() error {
	if err := ndb.tx.Commit(); err != nil {
		return ndb.tx.Rollback()
	}
	return nil
}
