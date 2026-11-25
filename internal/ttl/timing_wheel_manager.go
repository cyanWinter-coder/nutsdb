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
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/nutsdb/nutsdb/internal/core"
)

// keyId identifies a key across the database: the bucket it lives in and
// the FNV-1a hash of its bytes. Two registrations of the same (bucket, key)
// always resolve to the same keyId.
type keyId struct {
	bucketId uint64
	keyHash  uint64
}

// wheelEntry is the single source of truth for one registered key.
// At most one wheelEntry exists per keyId at any time; overwriting a key
// replaces the whole entry and cancels the timer of the previous version,
// so a stale expiration can never touch a newer value.
type wheelEntry struct {
	key       []byte // copied key bytes, used to build expiration events
	ds        uint16 // data structure type
	timestamp uint64 // write timestamp of this version, carried on events
	version   uint64 // monotonically increasing per keyId, guards stale timers
	lap       uint32 // remaining full-wheel rotations for long TTLs
	timer     *timingwheel.Timer
}

// keyState tracks the per-keyId version counter across the whole lifetime of
// the key, including gaps where no entry is registered (delete followed by
// re-create). Keeping the counter outside wheelEntry guarantees a version is
// never reused, so a timer scheduled before a delete can never match an entry
// installed afterwards.
type keyState struct {
	entry       *wheelEntry
	nextVersion uint64
}

// hashKey computes a 64-bit hash of the key using FNV-1a algorithm.
// FNV-1a is chosen for its simplicity and good distribution properties.
func hashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// TimingWheelManager manages active TTL expiration using a timing wheel.
// It wraps the RussellLuo/timingwheel library and integrates it with nutsdb's
// TTL infrastructure, providing key registration, deregistration, and expiration
// event generation.
//
// Concurrency model: every mutation of entries (register, deregister,
// expiration) is serialized through mu. A timer callback captures the keyId
// and the entry version it was scheduled for; when it fires, the callback
// re-reads the current entry under mu and only emits an event when the version
// still matches, so expiration of an old version can never delete a value
// written later (including writes sharing the same millisecond timestamp).
type TimingWheelManager struct {
	wheel        *timingwheel.TimingWheel
	queue        *expirationQueue
	enabled      bool
	slotDuration time.Duration
	wheelSize    int64 // number of slots in the wheel

	// mu guards states and the stopped flag.
	mu      sync.Mutex
	states  map[keyId]*keyState
	stopped bool

	// clock anchors the absolute expiration instant. It is the service clock
	// (a MockClock in deterministic tests, the real clock in production).
	clock Clock

	// Retry buffer for failed queue pushes
	retryBuffer  chan *ExpirationEvent
	maxRetrySize int

	// Metrics
	registeredKeys   atomic.Int64
	triggeredDeletes atomic.Int64
	failedPushes     atomic.Int64

	// Lifecycle management
	lifecycle core.ComponentLifecycle
}

// TimingWheelMetrics contains metrics for timing wheel operations.
type TimingWheelMetrics struct {
	RegisteredKeys   int64 // Total keys currently registered
	TriggeredDeletes int64 // Total deletions triggered by the wheel
	FailedPushes     int64 // Failed pushes to expiration queue
}

// NewTimingWheelManager creates a new timing wheel manager.
// If the timing wheel is disabled in config, it returns a manager with enabled=false
// that will no-op on all operations.
func NewTimingWheelManager(config Config, queue *expirationQueue, clk Clock) *TimingWheelManager {
	config.Validate()

	mgr := &TimingWheelManager{
		queue:        queue,
		enabled:      config.EnableTimingWheel,
		slotDuration: config.WheelSlotDuration,
		wheelSize:    int64(config.WheelSize),
		states:       make(map[keyId]*keyState),
		clock:        clk,
		maxRetrySize: max(1, config.QueueSize/10), // At least 1, or 10% of queue size
	}

	if config.EnableTimingWheel {
		mgr.wheel = timingwheel.NewTimingWheel(
			config.WheelSlotDuration,
			int64(config.WheelSize),
		)

		mgr.retryBuffer = make(chan *ExpirationEvent, mgr.maxRetrySize)
	}

	return mgr
}

// Name returns the component name for lifecycle management.
func (m *TimingWheelManager) Name() string {
	return "TimingWheelManager"
}

// Start starts the timing wheel ticker.
// If the timing wheel is disabled, this is a no-op.
// Returns an error if the component has already been started or stopped.
func (m *TimingWheelManager) Start(ctx context.Context) error {
	if !m.enabled || m.wheel == nil {
		return nil
	}

	if err := m.lifecycle.Start(ctx); err != nil {
		return err
	}

	m.wheel.Start()

	return nil
}

// Stop stops the timing wheel immediately without processing remaining slots.
// It cancels every pending timer and clears the registry.
// This method is idempotent - calling Stop() multiple times is safe.
func (m *TimingWheelManager) Stop(timeout time.Duration) error {
	if !m.enabled || m.wheel == nil {
		return nil
	}

	// Check if already stopped to avoid calling wheel.Stop() multiple times
	if m.lifecycle.IsStopped() {
		return nil
	}

	m.wheel.Stop()

	m.mu.Lock()
	for id, state := range m.states {
		if state.entry != nil && state.entry.timer != nil {
			state.entry.timer.Stop()
		}
		delete(m.states, id)
	}
	m.stopped = true
	m.mu.Unlock()
	m.registeredKeys.Store(0)

	return m.lifecycle.Stop(timeout)
}

// RegisterKey registers a key for active expiration in the timing wheel.
// Returns true if registered successfully, false if wheel is disabled.
//
// Every call registers a fresh expiration item for the key and cancels the
// previously registered item (if any), so only the latest version of a key
// has an active timer. The version counter makes stale timers harmless even
// when their callback races with this registration.
//
// The method calculates the appropriate slot position based on the key's TTL
// and schedules a timer callback for expiration. For TTLs exceeding the wheel's
// coverage, it uses lap counters to handle multiple wheel rotations.
func (m *TimingWheelManager) RegisterKey(
	bucketId uint64,
	key []byte,
	ds uint16,
	ttl uint32,
	timestamp uint64,
) bool {
	if !m.enabled || m.wheel == nil {
		return false
	}

	// Expiration is anchored at timestamp + ttl, so the same call works both
	// for live writes and for rebuilding the queue on restart with only the
	// remaining lifetime.
	expireAtMillis := int64(timestamp) + int64(ttl)*1000
	expirationDuration := time.Duration(expireAtMillis-m.nowMillis()) * time.Millisecond
	return m.installEntry(bucketId, key, ds, timestamp, expirationDuration)
}

// RegisterKeyWithRemainingTTL registers a key using an explicit remaining
// lifetime instead of the full original TTL. It is used when rebuilding the
// expiration queue after a restart: the record's absolute expiration instant
// (timestamp + originalTTL) stays unchanged, and only the time still to run
// is scheduled. remainingTTL == 0 means the item is already expired and the
// call deregisters any stale item instead of arming a timer.
func (m *TimingWheelManager) RegisterKeyWithRemainingTTL(
	bucketId uint64,
	key []byte,
	ds uint16,
	originalTTL uint32,
	timestamp uint64,
	remainingTTL uint32,
) bool {
	if !m.enabled || m.wheel == nil {
		return false
	}

	if remainingTTL == 0 {
		m.DeregisterKey(bucketId, key)
		return false
	}

	return m.installEntry(bucketId, key, ds, timestamp, time.Duration(remainingTTL)*time.Second)
}

// installEntry contains the shared scheduling logic. The event always carries
// the write timestamp, which expiration callbacks compare against the live
// record to reject stale items. The wheel is armed outside the mutex on
// purpose: when a timer is already due, the timing wheel library runs its
// task immediately in a new goroutine, and that task needs the same mutex.
func (m *TimingWheelManager) installEntry(
	bucketId uint64,
	key []byte,
	ds uint16,
	timestamp uint64,
	expirationDuration time.Duration,
) bool {
	id := keyId{bucketId: bucketId, keyHash: hashKey(key)}

	// Make a copy so the registry never aliases the caller's slice.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	if expirationDuration < m.slotDuration {
		// Never schedule below the wheel granularity; the slot tick is the
		// earliest the wheel can fire anyway.
		expirationDuration = m.slotDuration
	}

	wheelCoverage := m.slotDuration * time.Duration(m.wheelSize)

	var lapCounter uint32
	if expirationDuration > wheelCoverage {
		lapCounter = uint32(expirationDuration / wheelCoverage)
		expirationDuration = expirationDuration % wheelCoverage
		if expirationDuration == 0 {
			// If exactly divisible, we need one less lap and full wheel duration
			lapCounter--
			expirationDuration = wheelCoverage
		}
	}

	// Phase 1: under the lock, cancel the previous timer and reserve the
	// version for the new entry.
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return false
	}
	state, exists := m.states[id]
	if !exists {
		state = &keyState{}
		m.states[id] = state
	}
	if state.entry != nil && state.entry.timer != nil {
		state.entry.timer.Stop()
	}
	version := state.nextVersion
	state.nextVersion++
	m.mu.Unlock()

	// Phase 2: arm the wheel without holding mu.
	timer := m.wheel.AfterFunc(expirationDuration, func() {
		m.onKeyExpired(id, version)
	})

	// Phase 3: publish the entry. A timer that fired during phase 2 would
	// have found no matching entry yet; once it is published it becomes the
	// authoritative item and the (already consumed) one-shot callback cannot
	// run again.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		timer.Stop()
		return false
	}

	state = m.states[id] // re-read; a concurrent Deregister may have removed it
	if state == nil {
		state = &keyState{nextVersion: version + 1}
		m.states[id] = state
		timer.Stop()
		return false
	}

	state.entry = &wheelEntry{
		key:       keyCopy,
		ds:        ds,
		timestamp: timestamp,
		lap:       lapCounter,
		version:   version,
		timer:     timer,
	}
	m.registeredKeys.Store(m.liveEntryCountLocked())

	return true
}

// liveEntryCountLocked returns the number of keys with an active entry.
// Callers must hold mu.
func (m *TimingWheelManager) liveEntryCountLocked() int64 {
	var n int64
	for _, state := range m.states {
		if state.entry != nil {
			n++
		}
	}
	return n
}

// nowMillis returns the current time in milliseconds from the injected clock.
// The timing wheel dispatches on real elapsed time, but the expiration anchor
// follows the service clock so deterministic MockClock tests stay reproducible.
func (m *TimingWheelManager) nowMillis() int64 {
	return m.clock.NowMillis()
}

// DeregisterKey removes the key's active expiration item and cancels its
// timer. This must be called when a key is deleted, overwritten without TTL
// (made persistent), or otherwise invalidated before it expires.
// Returns true if the key had a registered item that was removed.
func (m *TimingWheelManager) DeregisterKey(bucketId uint64, key []byte) bool {
	if !m.enabled || m.wheel == nil {
		return false
	}

	return m.removeEntry(keyId{bucketId: bucketId, keyHash: hashKey(key)})
}

// removeEntry cancels and deletes the entry for id. Callers must not hold mu.
// The keyState (and its version counter) is retained so a later re-registration
// cannot reuse a version number a stale timer may still carry.
func (m *TimingWheelManager) removeEntry(id keyId) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[id]
	if !exists || state.entry == nil {
		return false
	}
	if state.entry.timer != nil {
		state.entry.timer.Stop()
	}
	state.entry = nil
	m.registeredKeys.Store(m.liveEntryCountLocked())
	return true
}

// processRetries attempts to drain the retry buffer and push failed events
// to the main expiration queue. This should be called before processing new
// slot expirations to prioritize retried events.
//
// The method drains events from the retry buffer and attempts to push them
// to the main queue. If a push fails (queue still full), the event is put
// back in the retry buffer. Processing stops if the retry buffer is full
// to avoid blocking.
func (m *TimingWheelManager) processRetries() {
	if !m.enabled || m.retryBuffer == nil {
		return
	}

	for {
		select {
		case event := <-m.retryBuffer:
			if !m.queue.push(event) {
				select {
				case m.retryBuffer <- event:
					return
				default:
					// Retry buffer is full, stop processing; event is dropped and lazy deletion will clean up
					return
				}
			} else {
				m.triggeredDeletes.Add(1)
			}
		default:
			return
		}
	}
}

// onKeyExpired is the callback invoked when a key's timer fires.
//
// The captured version is compared against the currently registered entry:
// a mismatch means the key was overwritten or deregistered after this timer
// was scheduled, so the callback returns without touching the new value.
// Multi-lap TTLs reschedule themselves under the same version check.
func (m *TimingWheelManager) onKeyExpired(id keyId, version uint64) {
	// Process retries before handling new expirations to prioritize them
	m.processRetries()

	var (
		event         *ExpirationEvent
		rearm         bool
		wheelCoverage time.Duration
	)

	m.mu.Lock()
	state, exists := m.states[id]
	if !exists || state.entry == nil || state.entry.version != version {
		// Stale timer: the key was deregistered or overwritten after this
		// timer was scheduled. Nothing to do, and the current entry (if any)
		// must be left untouched.
		m.mu.Unlock()
		return
	}
	entry := state.entry

	if entry.lap > 0 {
		// Decrement the lap and prepare a re-arm; the actual AfterFunc call
		// happens after unlocking (the library may run the task inline).
		entry.lap--
		rearm = true
		wheelCoverage = m.slotDuration * time.Duration(m.wheelSize)
		m.mu.Unlock()
	} else {
		// This version has genuinely expired: take ownership of its data and
		// remove the registry entry before releasing the lock, so a concurrent
		// overwrite cannot observe a half-deleted entry.
		event = &ExpirationEvent{
			BucketId:  id.bucketId,
			Key:       entry.key,
			Ds:        entry.ds,
			Timestamp: entry.timestamp,
		}
		state.entry = nil
		m.registeredKeys.Store(m.liveEntryCountLocked())
		m.mu.Unlock()
	}

	if rearm {
		// Re-arm for another full wheel rotation under the same version. If
		// the entry is replaced meanwhile, its version changes and the new
		// timer's callback is dropped by the guard at the top of this method.
		timer := m.wheel.AfterFunc(wheelCoverage, func() {
			m.onKeyExpired(id, version)
		})

		m.mu.Lock()
		// Only adopt the timer if this exact version is still live; an
		// overwrite during the unlocked window already cancelled/replaced it.
		if current, ok := m.states[id]; ok && current.entry != nil && current.entry.version == version {
			current.entry.timer = timer
		} else {
			timer.Stop()
		}
		m.mu.Unlock()
		return
	}

	if !m.queue.push(event) {
		m.failedPushes.Add(1)
		select {
		case m.retryBuffer <- event:
		default:
			// Retry buffer also full; event is dropped and lazy deletion will handle it
		}
	} else {
		m.triggeredDeletes.Add(1)
	}
}

// GetMetrics returns current timing wheel metrics.
// This provides visibility into the timing wheel's operations for monitoring
// and troubleshooting purposes.
func (m *TimingWheelManager) GetMetrics() TimingWheelMetrics {
	return TimingWheelMetrics{
		RegisteredKeys:   m.registeredKeys.Load(),
		TriggeredDeletes: m.triggeredDeletes.Load(),
		FailedPushes:     m.failedPushes.Load(),
	}
}

// lookupEntry returns a snapshot copy of the registered entry for a key, or
// nil when the key has no active item. Intended for tests and in-package
// diagnostics; callers must not mutate the returned value.
func (m *TimingWheelManager) lookupEntry(bucketId uint64, key []byte) *wheelEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, exists := m.states[keyId{bucketId: bucketId, keyHash: hashKey(key)}]
	if !exists || state.entry == nil {
		return nil
	}
	snapshot := *state.entry
	return &snapshot
}

// fireExpirationForTest deterministically runs the expiration callback of the
// currently registered version of a key. It mirrors what the timing wheel
// would do when the slot fires, without depending on real time.
func (m *TimingWheelManager) fireExpirationForTest(bucketId uint64, key []byte) bool {
	m.mu.Lock()
	state, exists := m.states[keyId{bucketId: bucketId, keyHash: hashKey(key)}]
	if !exists || state.entry == nil {
		m.mu.Unlock()
		return false
	}
	version := state.entry.version
	id := keyId{bucketId: bucketId, keyHash: hashKey(key)}
	m.mu.Unlock()

	m.onKeyExpired(id, version)
	return true
}
