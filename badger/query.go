// Copyright 2020 RTrade Technologies Ltd
//
// licensed under GNU AFFERO GENERAL PUBLIC LICENSE;
// you may not use this file except in compliance with the License;
// You may obtain the license via the LICENSE file in the repository root;
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dsbadger

import (
	badger "github.com/dgraph-io/badger/v2"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

/*
	This file contians an optimize query function as the one included from upstream allocates a lot of memory
	when parsing very large datasets. A key-value store with approximately 1.7TB of data stored requires a runtime allocation
	of 2GB to simply iterate over all of its keys
*/

func (t *txn) query(q dsq.Query) (dsq.Results, error) {
	if t.ds.closed.IsSet() {
		return nil, ErrClosed
	}
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = !q.KeysOnly
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		opt.Prefix = []byte(prefix + "/")
	}
	// Handle ordering
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			opt.Reverse = true
		default:
			baseQuery := q
			baseQuery.Limit = 0
			baseQuery.Offset = 0
			baseQuery.Orders = nil
			res, err := t.query(baseQuery)
			if err != nil {
				return nil, err
			}
			res = dsq.ResultsReplaceQuery(res, q)
			naiveQuery := q
			naiveQuery.Prefix = ""
			naiveQuery.Filters = nil
			return dsq.NaiveQueryApply(naiveQuery, res), nil
		}
	}
	var (
		it      = t.txn.NewIterator(opt)
		item    *badger.Item
		entries []dsq.Entry
	)
	if t.implicit {
		defer t.discard()
	}
	defer it.Close()
	it.Rewind()
	for skipped := 0; skipped < q.Offset && it.Valid(); it.Next() {
		if len(q.Filters) == 0 {
			skipped++
			continue
		}
		item = it.Item()
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
		if q.KeysOnly {
			_ = check(nil)
		} else {
			_ = item.Value(check)
		}
		if !matches {
			skipped++
		}
	}
	var value []byte
	for sent := 0; (q.Limit <= 0 || sent < q.Limit) && it.Valid(); it.Next() {
		item = it.Item()
		entry := dsq.Entry{Key: string(item.Key())}
		var copyErr error
		if !q.KeysOnly {
			value, copyErr = item.ValueCopy(value)
			if copyErr == nil {
				entry.Value = value
			}
			entry.Size = len(entry.Value)
		} else {
			entry.Size = int(item.ValueSize())
		}
		if q.ReturnExpirations {
			entry.Expiration = expires(item)
		}
		if copyErr == nil && filter(q.Filters, entry) {
			continue
		}
		entries = append(entries, entry)
		sent++
	}
	return dsq.ResultsWithEntries(q, entries), nil
}
