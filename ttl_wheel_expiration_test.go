// Copyright 2025 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openDBWithWheel opens a database with the timing wheel enabled and a
// MockClock installed, so expiration behavior stays deterministic while the
// wheel's real timer goroutine still exercises the full active-expiration
// path.
func openDBWithWheel(t *testing.T, mc ttl.Clock, slot time.Duration, wheelSize int) *DB {
	t.Helper()
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "nutsdb-wheel-test")
	opts.TTLConfig.EnableTimingWheel = true
	opts.TTLConfig.WheelSlotDuration = slot
	opts.TTLConfig.WheelSize = wheelSize
	opts.clock = mc

	db, err := Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !db.IsClose() {
			require.NoError(t, db.Close())
		}
	})
	return db
}

// wheelState returns the manager's view of a key for white-box assertions.
func wheelState(t *testing.T, db *DB, bucketId uint64, key []byte) (registered bool, version uint64) {
	t.Helper()
	require.NotNil(t, db.ttlService)
	entry := db.ttlService.GetTimingWheelManagerForTest().LookupEntryForTest(bucketId, key)
	if entry == nil {
		return false, 0
	}
	return true, entry.Version
}

// TestTTLWheel_OverwriteShortTTLWithLongTTL is the exact regression from the
// bug report: short-TTL write followed by a long-TTL overwrite must leave the
// new value readable after the old deadline passes.
func TestTTLWheel_OverwriteShortTTLWithLongTTL(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	// 50ms slots, 60 slots = 3s coverage; both TTLs fit inside one coverage.
	db := openDBWithWheel(t, mc, 50*time.Millisecond, 60)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	key := []byte("overwrite-key")

	txPut(t, db, bucket, key, []byte("v1"), 1, nil, nil)
	registered, v1 := wheelState(t, db, 1, key)
	require.True(t, registered, "short-TTL write must register an expiration item")

	// Overwrite with a much longer TTL before the first deadline.
	txPut(t, db, bucket, key, []byte("v2"), 10, nil, nil)
	registered, v2 := wheelState(t, db, 1, key)
	require.True(t, registered)
	assert.Greater(t, v2, v1, "overwrite must install a new versioned item")

	metrics := db.ttlService.GetTimingWheelManagerForTest().GetMetrics()
	assert.Equal(t, int64(1), metrics.RegisteredKeys, "only one item per key after overwrite")

	// Cross the original short deadline: the stale timer must not delete v2.
	mc.AdvanceTime(1500 * time.Millisecond)
	time.Sleep(150 * time.Millisecond) // let the wheel process elapsed slots
	txGet(t, db, bucket, key, []byte("v2"), nil)

	// The registered item is still the new version; no dangling entry left.
	registered, version := wheelState(t, db, 1, key)
	require.True(t, registered, "new value must keep its own expiration item")
	assert.Equal(t, v2, version)

	// Cross the long deadline: v2 now expires. With a MockClock the wheel's
	// real timers don't fire, so the first Get drives lazy expiration; the
	// important contract is that the value actually disappears.
	mc.AdvanceTime(11 * time.Second)
	txGet(t, db, bucket, key, nil, ErrKeyNotFound)

	// The expiration callback (lazy or active) always drops the registry item.
	require.Eventually(t, func() bool {
		registered, _ = wheelState(t, db, 1, key)
		return !registered
	}, 2*time.Second, 20*time.Millisecond, "expiration must remove the registry item")
}

// TestTTLWheel_OverwriteWithSameTimestamp proves the version guard works even
// when both writes land in the same millisecond (identical timestamps).
func TestTTLWheel_OverwriteWithSameTimestamp(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	db := openDBWithWheel(t, mc, 50*time.Millisecond, 60)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	key := []byte("same-ts-key")
	ts := uint64(mc.NowMillis())

	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.PutWithTimestamp(bucket, key, []byte("v1"), 1, ts)
	}))
	_, v1 := wheelState(t, db, 1, key)
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.PutWithTimestamp(bucket, key, []byte("v2"), 10, ts)
	}))
	_, v2 := wheelState(t, db, 1, key)
	assert.Greater(t, v2, v1)

	mc.AdvanceTime(1500 * time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	txGet(t, db, bucket, key, []byte("v2"), nil)
}

// TestTTLWheel_DeleteRemovesExpirationItem verifies requirement 3 for the
// delete path.
func TestTTLWheel_DeleteRemovesExpirationItem(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	db := openDBWithWheel(t, mc, 50*time.Millisecond, 60)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	key := []byte("delete-key")
	txPut(t, db, bucket, key, []byte("v"), 10, nil, nil)
	registered, _ := wheelState(t, db, 1, key)
	require.True(t, registered)

	txDel(t, db, bucket, key, nil)
	registered, _ = wheelState(t, db, 1, key)
	assert.False(t, registered, "delete must remove the expiration item")

	// Well past the original deadline: nothing can resurrect a delete event.
	mc.AdvanceTime(11 * time.Second)
	time.Sleep(150 * time.Millisecond)
	txGet(t, db, bucket, key, nil, ErrKeyNotFound)
	assert.Equal(t, int64(0), db.ttlService.GetTimingWheelManagerForTest().GetMetrics().RegisteredKeys)
}

// TestTTLWheel_PersistRemovesExpirationItem verifies requirement 3 for the
// TTL -> persistent transition.
func TestTTLWheel_PersistRemovesExpirationItem(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	db := openDBWithWheel(t, mc, 50*time.Millisecond, 60)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	key := []byte("persist-key")
	txPut(t, db, bucket, key, []byte("v"), 1, nil, nil)
	registered, _ := wheelState(t, db, 1, key)
	require.True(t, registered)

	txPersist(t, db, bucket, key, nil)
	registered, _ = wheelState(t, db, 1, key)
	assert.False(t, registered, "making a key persistent must remove its expiration item")

	mc.AdvanceTime(11 * time.Second)
	time.Sleep(150 * time.Millisecond)
	// Persistent value survives what used to be its TTL deadline.
	txGet(t, db, bucket, key, []byte("v"), nil)
}

// TestTTLWheel_RestartRebuildsWithRemainingTTL covers requirement 4: after
// reopen, timers reflect the remaining lifetime and already-expired items are
// cleaned during the rebuild rather than scheduled.
func TestTTLWheel_RestartRebuildsWithRemainingTTL(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	dir := filepath.Join(t.TempDir(), "nutsdb-restart-test")

	opts := DefaultOptions
	opts.Dir = dir
	opts.TTLConfig.EnableTimingWheel = true
	opts.TTLConfig.WheelSlotDuration = 50 * time.Millisecond
	opts.TTLConfig.WheelSize = 60
	opts.clock = mc

	db, err := Open(opts)
	require.NoError(t, err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	liveKey := []byte("live-key")     // ttl 10s
	expiringKey := []byte("expiring") // ttl 1s
	txPut(t, db, bucket, liveKey, []byte("live"), 10, nil, nil)
	txPut(t, db, bucket, expiringKey, []byte("gone"), 1, nil, nil)
	require.NoError(t, db.Close())

	// Reopen 3 simulated seconds later: liveKey has ~7s left, expiringKey is
	// already past its deadline.
	mc.AdvanceTime(3 * time.Second)

	db2, err := Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	mgr := db2.ttlService.GetTimingWheelManagerForTest()
	require.NotNil(t, mgr)

	liveEntry := mgr.LookupEntryForTest(1, liveKey)
	require.NotNil(t, liveEntry, "unexpired key must be re-registered on rebuild")
	assert.Equal(t, int64(1), mgr.GetMetrics().RegisteredKeys,
		"the already-expired key must not be re-registered")

	// expiringKey was expired at rebuild: it must be gone from the index too.
	txGet(t, db2, bucket, expiringKey, nil, ErrKeyNotFound)
	// liveKey remains readable.
	txGet(t, db2, bucket, liveKey, []byte("live"), nil)

	// Advance only the remaining lifetime (not the full original 10s): the
	// rebuilt timer must fire around the original absolute deadline.
	mc.AdvanceTime(7500 * time.Millisecond)
	require.Eventually(t, func() bool {
		var found bool
		_ = db2.View(func(tx *Tx) error {
			_, err := tx.Get(bucket, liveKey)
			found = err == nil
			return nil
		})
		return !found
	}, 2*time.Second, 50*time.Millisecond,
		"rebuilt key must expire on its original schedule, not a full TTL after restart")
}

// TestTTLWheel_ConcurrentOverwriteAndExpiration stresses requirement 5:
// expiration callbacks racing with concurrent overwrites must never produce
// dangling registry entries or delete fresh values.
func TestTTLWheel_ConcurrentOverwriteAndExpiration(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	db := openDBWithWheel(t, mc, 20*time.Millisecond, 100)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	key := []byte("race-key")
	txPut(t, db, bucket, key, []byte("v0"), 1, nil, nil)

	const writers = 8
	const rounds = 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				ttl := uint32(1 + (r % 5))
				_ = db.Update(func(tx *Tx) error {
					return tx.Put(bucket, key, []byte("fresh"), ttl)
				})
				// Occasional deletes interleave with overwrites.
				if r%7 == 0 {
					_ = db.Update(func(tx *Tx) error {
						_ = tx.Delete(bucket, key)
						return nil
					})
				}
				// Recreate after delete so the loop always has a live key.
				_ = db.Update(func(tx *Tx) error {
					return tx.Put(bucket, key, []byte("fresh"), 10)
				})
			}
		}(w)
	}

	// Concurrently advance time in small steps, driving wheel expirations.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				mc.AdvanceTime(100 * time.Millisecond)
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	close(stop)

	// Final state: key exists with value "fresh" and long TTL. Either it is
	// registered exactly once, or (if the last write already expired) absent;
	// what is never acceptable is a dangling entry without an index record.
	mgr := db.ttlService.GetTimingWheelManagerForTest()
	assert.LessOrEqual(t, mgr.GetMetrics().RegisteredKeys, int64(1),
		"at most one registry item per key regardless of interleaving")

	if entry := mgr.LookupEntryForTest(1, key); entry != nil {
		var value []byte
		_ = db.View(func(tx *Tx) error {
			v, err := tx.Get(bucket, key)
			if err == nil {
				value = v
			}
			return nil
		})
		require.NotNil(t, value, "registered item must reference an existing record")
		assert.Equal(t, []byte("fresh"), value)
	}
}

// TestTTLWheel_BatchOverwriteKeepsSingleItem verifies mixed batches and
// repeated overwrites of many keys never leak stale items.
func TestTTLWheel_BatchOverwriteKeepsSingleItem(t *testing.T) {
	mc := ttl.NewMockClock(time.Now().UnixMilli())
	db := openDBWithWheel(t, mc, 50*time.Millisecond, 60)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	const keyCount = 25
	keys := make([][]byte, keyCount)
	for i := range keys {
		keys[i] = []byte("batch-key-" + string(rune('a'+i)))
		txPut(t, db, bucket, keys[i], []byte("v1"), 1, nil, nil)
	}
	require.Equal(t, int64(keyCount),
		db.ttlService.GetTimingWheelManagerForTest().GetMetrics().RegisteredKeys)

	// Overwrite every key with a long TTL: counts must not double.
	for _, key := range keys {
		txPut(t, db, bucket, key, []byte("v2"), 60, nil, nil)
	}
	assert.Equal(t, int64(keyCount),
		db.ttlService.GetTimingWheelManagerForTest().GetMetrics().RegisteredKeys,
		"overwriting keys must replace, not accumulate expiration items")

	// Delete half: count drops accordingly, no negative drift.
	deleted := 0
	for i, key := range keys {
		if i%2 == 0 {
			txDel(t, db, bucket, key, nil)
			deleted++
		}
	}
	assert.Equal(t, int64(keyCount-deleted),
		db.ttlService.GetTimingWheelManagerForTest().GetMetrics().RegisteredKeys)

	mc.AdvanceTime(1500 * time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	// Surviving keys still hold v2; deleted ones stay gone.
	for i, key := range keys {
		if i%2 == 0 {
			txGet(t, db, bucket, key, nil, ErrKeyNotFound)
		} else {
			txGet(t, db, bucket, key, []byte("v2"), nil)
		}
	}
}
