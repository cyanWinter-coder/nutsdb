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
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
)

type BatchExpiredCallback func(events []*ExpirationEvent)

// Service provides TTL checking and batched expiration deletion.
// Expired keys are detected lazily during reads and collected into a queue,
// then batch-deleted by a background goroutine.
type Service struct {
	lifecycle       core.ComponentLifecycle
	checker         *Checker
	clock           Clock
	expiredCallback BatchExpiredCallback
	queue           *expirationQueue
	batchSize       int
	batchTimeout    time.Duration
	wheelManager    *TimingWheelManager // nil if timing wheel is disabled
}

func NewService(clk Clock, config Config, callback BatchExpiredCallback) *Service {
	config.Validate()

	chk := NewChecker(clk)
	queue := newExpirationQueue(config.QueueSize)

	var wheelManager *TimingWheelManager
	if config.EnableTimingWheel {
		wheelManager = NewTimingWheelManager(config, queue, clk)
	}

	service := &Service{
		checker:         chk,
		clock:           clk,
		expiredCallback: callback,
		queue:           queue,
		batchSize:       config.BatchSize,
		batchTimeout:    config.BatchTimeout,
		wheelManager:    wheelManager,
	}

	chk.SetExpiredCallback(func(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
		service.onExpired(bucketId, key, ds, timestamp)
	})

	return service
}

// NowMillis returns the current time in milliseconds via the internal clock.
// This facade method avoids exposing the Clock hierarchy to callers.
func (s *Service) NowMillis() int64 {
	return s.clock.NowMillis()
}

// NowSeconds returns the current time in seconds via the internal clock.
// This facade method avoids exposing the Clock hierarchy to callers.
func (s *Service) NowSeconds() int64 {
	return s.clock.NowSeconds()
}

// IsExpired checks whether a record is expired via the internal checker.
// This facade method avoids exposing the Checker hierarchy to callers.
func (s *Service) IsExpired(ttl uint32, timestamp uint64) bool {
	return s.checker.IsExpired(ttl, timestamp)
}

// GetChecker returns the internal checker.
// Internal use only for internal data structures.
// Deprecated: root package code should use Service.IsExpired instead.
func (s *Service) GetChecker() *Checker {
	return s.checker
}

// SetClock updates the service clock and propagates it to the checker and
// the timing wheel manager. Intended for tests that need deterministic time
// control.
func (s *Service) SetClock(clk Clock) {
	s.clock = clk
	if s.checker != nil {
		s.checker.SetClock(clk)
	}
	if s.wheelManager != nil {
		s.wheelManager.clock = clk
	}
}

func (s *Service) onExpired(bucketId uint64, key []byte, ds uint16, timestamp uint64) {
	event := &ExpirationEvent{
		BucketId:  bucketId,
		Key:       key,
		Ds:        ds,
		Timestamp: timestamp,
	}
	s.queue.push(event)
}

func (s *Service) processExpirationEvents(ctx context.Context) {
	batch := make([]*ExpirationEvent, 0, s.batchSize)
	timer := time.NewTimer(s.batchTimeout)
	defer timer.Stop()

	flushBatch := func() {
		if len(batch) > 0 && s.expiredCallback != nil {
			s.expiredCallback(batch)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			return

		case event, ok := <-s.queue.events:
			if !ok {
				flushBatch()
				return
			}

			batch = append(batch, event)

			if len(batch) >= s.batchSize {
				flushBatch()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(s.batchTimeout)
			}

		case <-timer.C:
			flushBatch()
			timer.Reset(s.batchTimeout)
		}
	}
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.lifecycle.Start(ctx); err != nil {
		return err
	}

	s.lifecycle.Go(s.processExpirationEvents)

	if s.wheelManager != nil {
		if err := s.wheelManager.Start(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) Stop(timeout time.Duration) error {
	if s.wheelManager != nil {
		if err := s.wheelManager.Stop(timeout); err != nil {
			return err
		}
	}

	s.queue.close()
	return s.lifecycle.Stop(timeout)
}

func (s *Service) Name() string {
	return "TTLService"
}

// RegisterKeyForActiveExpiration registers a key in the timing wheel for active expiration.
// Should be called when a key with TTL is written to the database.
// Any previously registered item for the same key is cancelled first, so at
// most the latest write version has an active timer.
// If the timing wheel is disabled, this is a no-op.
func (s *Service) RegisterKeyForActiveExpiration(
	bucketId uint64,
	key []byte,
	ds uint16,
	ttl uint32,
	timestamp uint64,
) {
	if s.wheelManager != nil {
		s.wheelManager.RegisterKey(bucketId, key, ds, ttl, timestamp)
	}
}

// DeregisterKeyFromActiveExpiration removes a key from the timing wheel.
// Should be called when a key is deleted before expiration, made persistent,
// or otherwise invalidated.
// If the timing wheel is disabled, this is a no-op.
func (s *Service) DeregisterKeyFromActiveExpiration(bucketId uint64, key []byte) {
	if s.wheelManager != nil {
		s.wheelManager.DeregisterKey(bucketId, key)
	}
}

// SyncKeyExpiration reconciles the active expiration item of a key with the
// outcome of a committed write:
//   - ttl > 0 and the entry is not a delete marker: register a fresh item
//     (cancelling any item left by a previous version of the key);
//   - ttl == Persistent or a delete marker: remove any existing item so a
//     stale timer cannot fire against the new state.
//
// If the timing wheel is disabled, this is a no-op.
func (s *Service) SyncKeyExpiration(bucketId uint64, key []byte, ds uint16, ttl uint32, timestamp uint64, isDelete bool) {
	if s.wheelManager == nil {
		return
	}
	if ttl > 0 && !isDelete {
		s.wheelManager.RegisterKey(bucketId, key, ds, ttl, timestamp)
	} else {
		s.wheelManager.DeregisterKey(bucketId, key)
	}
}

// RebuildKeyExpiration re-registers a key while opening the database,
// scheduling only the remaining lifetime instead of the full original TTL.
// Items whose expiration instant is already in the past are dropped, and the
// caller is responsible for cleaning their index entries.
// Returns true when a timer was armed, false when the item was expired or
// the wheel is disabled.
func (s *Service) RebuildKeyExpiration(bucketId uint64, key []byte, ds uint16, ttl uint32, timestamp uint64) bool {
	if s.wheelManager == nil || ttl == Persistent {
		return false
	}

	expireAtMillis := int64(timestamp) + int64(ttl)*1000
	remainingMillis := expireAtMillis - s.NowMillis()
	if remainingMillis <= 0 {
		// Already expired at rebuild time: clear any stale item and report
		// that the record must be reaped rather than scheduled.
		s.wheelManager.DeregisterKey(bucketId, key)
		return false
	}

	// Ceiling to whole seconds; the wheel only schedules second-granularity
	// TTLs anyway and firing one slot late is safe (expiration is rechecked
	// against the live record before deletion).
	remainingTTL := uint32((remainingMillis + 999) / 1000)
	return s.wheelManager.RegisterKeyWithRemainingTTL(bucketId, key, ds, ttl, timestamp, remainingTTL)
}

// WheelEntryView is an immutable snapshot of a registered expiration item
// exposed for white-box testing.
type WheelEntryView struct {
	Key       []byte
	Ds        uint16
	Timestamp uint64
	Version   uint64
	Lap       uint32
}

// GetTimingWheelManagerForTest exposes the wheel manager for white-box
// testing. Production code must use the Register/Deregister facade methods.
func (s *Service) GetTimingWheelManagerForTest() *TimingWheelManager {
	return s.wheelManager
}

// LookupEntryForTest returns a snapshot of the key's registered item, or nil
// when no active item exists. Test-only diagnostic.
func (m *TimingWheelManager) LookupEntryForTest(bucketId uint64, key []byte) *WheelEntryView {
	entry := m.lookupEntry(bucketId, key)
	if entry == nil {
		return nil
	}
	return &WheelEntryView{
		Key:       entry.key,
		Ds:        entry.ds,
		Timestamp: entry.timestamp,
		Version:   entry.version,
		Lap:       entry.lap,
	}
}
