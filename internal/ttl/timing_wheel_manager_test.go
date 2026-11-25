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

package ttl

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T, clk Clock, config Config) (*TimingWheelManager, *expirationQueue) {
	t.Helper()
	if clk == nil {
		clk = NewMockClock(1000000)
	}
	queue := newExpirationQueue(config.QueueSize)
	mgr := NewTimingWheelManager(config, queue, clk)
	return mgr, queue
}

func enabledTestConfig() Config {
	return Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 100 * time.Millisecond,
		WheelSize:         10,
		QueueSize:         100,
	}
}

func TestWheelEntrySize(t *testing.T) {
	var entry wheelEntry
	size := unsafe.Sizeof(entry)
	t.Logf("wheelEntry size: %d bytes", size)
	// Registry entry must stay compact: keyId-equivalent fields plus timer/lap.
	assert.LessOrEqual(t, size, uintptr(96), "wheelEntry size should stay bounded")
}

func TestHashKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"empty key", []byte{}},
		{"simple key", []byte("key")},
		{"long key", []byte("this-is-a-very-long-key-for-testing-purposes")},
		{"binary key", []byte{0x00, 0x01, 0x02, 0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := hashKey(tt.key)
			hash2 := hashKey(tt.key)
			assert.Equal(t, hash1, hash2, "hash should be deterministic")
		})
	}
}

func TestHashKeyUniqueness(t *testing.T) {
	assert.NotEqual(t, hashKey([]byte("key1")), hashKey([]byte("key2")),
		"different keys should produce different hashes")
}

func TestTimingWheelManager_RegisterAndLookup(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, _ := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("test-key")
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))

	entry := mgr.lookupEntry(bucketId, key)
	require.NotNil(t, entry)
	assert.Equal(t, ts, entry.timestamp)
	assert.Equal(t, key, entry.key)
	assert.Equal(t, uint64(0), entry.version, "first registration starts at version 0")
	assert.Equal(t, int64(1), mgr.GetMetrics().RegisteredKeys)

	// The registry must hold its own copy of the key.
	key[0] = 'X'
	entry = mgr.lookupEntry(bucketId, []byte("test-key"))
	require.NotNil(t, entry)
	assert.Equal(t, []byte("test-key"), entry.key, "registry must copy the key bytes")
}

func TestTimingWheelManager_DeregisterKey(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, _ := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("test-key")
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))
	require.NotNil(t, mgr.lookupEntry(bucketId, key))

	assert.True(t, mgr.DeregisterKey(bucketId, key))
	assert.Nil(t, mgr.lookupEntry(bucketId, key))
	assert.Equal(t, int64(0), mgr.GetMetrics().RegisteredKeys)

	// Idempotent: deregistering an unknown key returns false and does not panic.
	assert.False(t, mgr.DeregisterKey(bucketId, key))
}

func TestTimingWheelManager_DeregisterKey_WhenDisabled(t *testing.T) {
	config := Config{EnableTimingWheel: false, QueueSize: 100}
	mgr, _ := newTestManager(t, NewMockClock(1000000), config)

	assert.False(t, mgr.DeregisterKey(1, []byte("test-key")))
	assert.False(t, mgr.RegisterKey(1, []byte("test-key"), 1, 1, 1))
}

// TestTimingWheelManager_DeregisterKey_PreventsExpiration verifies that after
// deregistration, even a fired timer cannot produce an expiration event.
func TestTimingWheelManager_DeregisterKey_PreventsExpiration(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, queue := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("test-key")
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))
	require.True(t, mgr.DeregisterKey(bucketId, key))
	assert.False(t, mgr.fireExpirationForTest(bucketId, key))

	select {
	case event := <-queue.events:
		t.Errorf("unexpected expiration event for deregistered key: %+v", event)
	default:
	}
}

// TestTimingWheelManager_OnKeyExpired_EmitsEvent covers the normal expiration
// path: one event with the registration timestamp, entry removed afterwards.
func TestTimingWheelManager_OnKeyExpired_EmitsEvent(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, queue := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("test-key")
	ds := uint16(1)
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, ds, 1, ts))
	require.True(t, mgr.fireExpirationForTest(bucketId, key))

	select {
	case event := <-queue.events:
		assert.Equal(t, bucketId, event.BucketId)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, ds, event.Ds)
		assert.Equal(t, ts, event.Timestamp)
	default:
		t.Fatal("expected expiration event but none received")
	}

	assert.Equal(t, int64(1), mgr.GetMetrics().TriggeredDeletes)
	assert.Nil(t, mgr.lookupEntry(bucketId, key), "entry must be removed after expiration")
	assert.Equal(t, int64(0), mgr.GetMetrics().RegisteredKeys)
}

// TestTimingWheelManager_OnKeyExpired_QueueFull verifies the retry buffer
// still receives the event when the main queue is full.
func TestTimingWheelManager_OnKeyExpired_QueueFull(t *testing.T) {
	clk := NewMockClock(1000000)
	config := enabledTestConfig()
	config.QueueSize = 2
	mgr, queue := newTestManager(t, clk, config)

	for i := 0; i < config.QueueSize; i++ {
		queue.push(&ExpirationEvent{
			BucketId:  uint64(i),
			Key:       []byte("filler"),
			Ds:        1,
			Timestamp: uint64(clk.NowMillis()),
		})
	}

	bucketId := uint64(100)
	key := []byte("test-key")
	ts := uint64(clk.NowMillis())
	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))
	require.True(t, mgr.fireExpirationForTest(bucketId, key))

	assert.Equal(t, int64(1), mgr.GetMetrics().FailedPushes)
	select {
	case event := <-mgr.retryBuffer:
		assert.Equal(t, bucketId, event.BucketId)
		assert.Equal(t, key, event.Key)
		assert.Equal(t, ts, event.Timestamp)
	default:
		t.Fatal("expected event in retry buffer but none found")
	}
}

// TestTimingWheelManager_OnKeyExpired_MultiLap verifies long TTLs reschedule
// for each wheel rotation and only emit after the final lap.
func TestTimingWheelManager_OnKeyExpired_MultiLap(t *testing.T) {
	clk := NewMockClock(1000000)
	config := enabledTestConfig()
	// Wheel coverage = 100ms * 10 = 1s; ttl=3s spans multiple rotations.
	config.WheelSize = 10
	mgr, queue := newTestManager(t, clk, config)
	defer mgr.Stop(time.Second)

	bucketId := uint64(1)
	key := []byte("test-key")
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, 1, 3, ts))

	entry := mgr.lookupEntry(bucketId, key)
	require.NotNil(t, entry)
	require.Positive(t, entry.lap, "ttl beyond wheel coverage must use laps")
	laps := entry.lap
	version := entry.version

	// Fire the intermediate rotations: no events, lap count decreasing.
	for lap := laps; lap > 0; lap-- {
		require.True(t, mgr.fireExpirationForTest(bucketId, key))
		select {
		case event := <-queue.events:
			t.Errorf("unexpected early expiration event: %+v", event)
		default:
		}
		entry = mgr.lookupEntry(bucketId, key)
		require.NotNil(t, entry, "entry must survive intermediate laps")
		assert.Equal(t, lap-1, entry.lap)
		assert.Equal(t, version, entry.version, "laps must not change the version")
	}

	// Final lap emits the event.
	require.True(t, mgr.fireExpirationForTest(bucketId, key))
	select {
	case event := <-queue.events:
		assert.Equal(t, ts, event.Timestamp)
	default:
		t.Fatal("expected expiration event after full TTL but none found")
	}
}

// TestTimingWheelManager_OnKeyExpired_UnknownKeySkipped fires a callback for a
// keyId that has no registry entry (e.g. an already-deregistered timer).
func TestTimingWheelManager_OnKeyExpired_UnknownKeySkipped(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, queue := newTestManager(t, clk, enabledTestConfig())

	id := keyId{bucketId: 1, keyHash: hashKey([]byte("ghost"))}
	mgr.onKeyExpired(id, 0)

	select {
	case event := <-queue.events:
		t.Errorf("unexpected expiration event for unknown key: %+v", event)
	default:
	}
	assert.Equal(t, int64(0), mgr.GetMetrics().TriggeredDeletes)
}

// TestTimingWheelManager_OverwriteCancelsOldVersion is the core regression for
// "short TTL then long TTL": the stale timer must never delete the new value.
func TestTimingWheelManager_OverwriteCancelsOldVersion(t *testing.T) {
	clk := NewMockClock(1000000)
	// Wheel coverage = 50ms * 20 = 1s, so both TTLs fit in a single lap.
	config := Config{
		EnableTimingWheel: true,
		WheelSlotDuration: 50 * time.Millisecond,
		WheelSize:         20,
		QueueSize:         100,
	}
	mgr, queue := newTestManager(t, clk, config)

	bucketId := uint64(1)
	key := []byte("k")
	ds := uint16(1)
	ts1 := uint64(clk.NowMillis())

	// Version 1: short TTL.
	require.True(t, mgr.RegisterKey(bucketId, key, ds, 1, ts1))
	entryV1 := mgr.lookupEntry(bucketId, key)
	require.NotNil(t, entryV1)
	require.Equal(t, uint64(0), entryV1.version)

	// Version 2: long TTL, one millisecond later. The version counter, not
	// the timestamp, must tell the timers apart.
	clk.AdvanceTime(time.Millisecond)
	ts2 := uint64(clk.NowMillis())
	require.True(t, mgr.RegisterKey(bucketId, key, ds, 1, ts2))
	entryV2 := mgr.lookupEntry(bucketId, key)
	require.NotNil(t, entryV2)
	assert.Equal(t, uint64(1), entryV2.version)
	assert.Equal(t, int64(1), mgr.GetMetrics().RegisteredKeys,
		"overwrite must keep exactly one item per key")

	// The v1 timer fires after its short TTL. Its captured version (0) no
	// longer matches, so it must be a no-op: no event, v2 entry untouched.
	mgr.onKeyExpired(keyId{bucketId: bucketId, keyHash: hashKey(key)}, entryV1.version)
	select {
	case event := <-queue.events:
		t.Errorf("stale timer deleted the overwritten value: %+v", event)
	default:
	}
	entry := mgr.lookupEntry(bucketId, key)
	require.NotNil(t, entry, "new version must survive the old timer firing")
	assert.Equal(t, entryV2.version, entry.version)
	assert.Equal(t, ts2, entry.timestamp)

	// The current version still expires normally at its own deadline.
	require.True(t, mgr.fireExpirationForTest(bucketId, key))
	select {
	case event := <-queue.events:
		assert.Equal(t, ts2, event.Timestamp)
	default:
		t.Fatal("expected the current version to expire at its own deadline")
	}
}

// TestTimingWheelManager_OverwriteSameTimestamp still distinguishes versions
// when both writes land in the same millisecond.
func TestTimingWheelManager_OverwriteSameTimestamp(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, queue := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("k")
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))
	v1 := mgr.lookupEntry(bucketId, key)
	require.True(t, mgr.RegisterKey(bucketId, key, 1, 100, ts))

	// Stale v1 timer with the identical timestamp: version mismatch saves it.
	mgr.onKeyExpired(keyId{bucketId: bucketId, keyHash: hashKey(key)}, v1.version)
	select {
	case event := <-queue.events:
		t.Errorf("stale timer fired despite identical timestamp: %+v", event)
	default:
	}
	require.NotNil(t, mgr.lookupEntry(bucketId, key))
}

// TestTimingWheelManager_RegisterThenDeregisterThenReregister verifies the
// version counter keeps moving forward across the full key lifecycle.
func TestTimingWheelManager_RegisterThenDeregisterThenReregister(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, _ := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("k")
	ts := uint64(clk.NowMillis())

	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))
	first := mgr.lookupEntry(bucketId, key)
	require.True(t, mgr.DeregisterKey(bucketId, key))
	require.True(t, mgr.RegisterKey(bucketId, key, 1, 1, ts))
	second := mgr.lookupEntry(bucketId, key)

	require.NotNil(t, first)
	require.NotNil(t, second)
	assert.Greater(t, second.version, first.version,
		"versions must never be reused even after deregistration")
}

// TestTimingWheelManager_RebuildWithRemainingTTL verifies restart semantics:
// only the remaining lifetime is scheduled and already-expired items are
// cleaned instead of armed.
func TestTimingWheelManager_RebuildWithRemainingTTL(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, queue := newTestManager(t, clk, enabledTestConfig())

	bucketId := uint64(1)
	key := []byte("rebuild-key")

	// Record written 300ms ago with ttl=1s: 700ms remain, fitting in one
	// wheel rotation (coverage = 100ms * 10 = 1s).
	originalTTL := uint32(1)
	timestamp := uint64(clk.NowMillis() - 300)

	remaining := uint32((int64(timestamp) + int64(originalTTL)*1000 - clk.NowMillis() + 999) / 1000)
	assert.Equal(t, uint32(1), remaining)

	require.True(t, mgr.RegisterKeyWithRemainingTTL(bucketId, key, 1, originalTTL, timestamp, remaining))
	entry := mgr.lookupEntry(bucketId, key)
	require.NotNil(t, entry)
	assert.Equal(t, timestamp, entry.timestamp, "event must carry the original timestamp")

	// Already expired: no timer armed, any stale entry removed.
	expiredKey := []byte("expired-key")
	require.True(t, mgr.RegisterKey(bucketId, expiredKey, 1, 1, uint64(clk.NowMillis())))
	assert.False(t, mgr.RegisterKeyWithRemainingTTL(bucketId, expiredKey, 1, 1,
		uint64(clk.NowMillis()-5000), 0))
	assert.Nil(t, mgr.lookupEntry(bucketId, expiredKey),
		"expired rebuild item must be cleaned immediately")

	// The live key still fires exactly one event with the original timestamp.
	require.True(t, mgr.fireExpirationForTest(bucketId, key))
	select {
	case event := <-queue.events:
		assert.Equal(t, timestamp, event.Timestamp)
	default:
		t.Fatal("expected rebuilt key to expire")
	}
}

// TestTimingWheelManager_Lifecycle covers start/stop including timer cleanup.
func TestTimingWheelManager_Lifecycle(t *testing.T) {
	t.Run("start and stop enabled wheel", func(t *testing.T) {
		mgr, _ := newTestManager(t, NewMockClock(1000000), enabledTestConfig())

		require.NoError(t, mgr.Start(context.Background()))
		assert.True(t, mgr.lifecycle.IsRunning())
		require.NoError(t, mgr.Stop(1*time.Second))
		assert.True(t, mgr.lifecycle.IsStopped())
	})

	t.Run("stop cancels pending timers and clears entries", func(t *testing.T) {
		mgr, _ := newTestManager(t, NewMockClock(1000000), enabledTestConfig())
		require.NoError(t, mgr.Start(context.Background()))

		require.True(t, mgr.RegisterKey(1, []byte("a"), 1, 100, 1))
		require.True(t, mgr.RegisterKey(1, []byte("b"), 1, 100, 1))

		require.NoError(t, mgr.Stop(1*time.Second))
		assert.Nil(t, mgr.lookupEntry(1, []byte("a")))
		assert.Nil(t, mgr.lookupEntry(1, []byte("b")))
		assert.Equal(t, int64(0), mgr.GetMetrics().RegisteredKeys)
	})

	t.Run("start and stop disabled wheel", func(t *testing.T) {
		config := Config{EnableTimingWheel: false, QueueSize: 10}
		mgr, _ := newTestManager(t, NewMockClock(1000000), config)

		require.NoError(t, mgr.Start(context.Background()))
		require.NoError(t, mgr.Stop(1*time.Second))
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		mgr, _ := newTestManager(t, NewMockClock(1000000), enabledTestConfig())
		require.NoError(t, mgr.Start(context.Background()))
		require.NoError(t, mgr.Stop(1*time.Second))
		require.NoError(t, mgr.Stop(1*time.Second))
	})

	t.Run("name returns correct value", func(t *testing.T) {
		mgr, _ := newTestManager(t, NewMockClock(1000000), enabledTestConfig())
		assert.Equal(t, "TimingWheelManager", mgr.Name())
	})
}

func TestTimingWheelManager_GetMetrics(t *testing.T) {
	clk := NewMockClock(1000000)
	mgr, _ := newTestManager(t, clk, enabledTestConfig())

	require.NoError(t, mgr.Start(context.Background()))
	defer mgr.Stop(1 * time.Second)

	metrics := mgr.GetMetrics()
	assert.Equal(t, int64(0), metrics.RegisteredKeys)

	require.True(t, mgr.RegisterKey(1, []byte("key1"), 1, 1, uint64(clk.NowMillis())))
	assert.Equal(t, int64(1), mgr.GetMetrics().RegisteredKeys)

	require.True(t, mgr.DeregisterKey(1, []byte("key1")))
	assert.Equal(t, int64(0), mgr.GetMetrics().RegisteredKeys)
}
