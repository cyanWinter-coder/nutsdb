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
	"sync"
	"sync/atomic"
)

// ExpirationEvent represents a key expiration event.
type ExpirationEvent struct {
	BucketId  uint64
	Key       []byte
	Ds        uint16
	Timestamp uint64
}

// expirationQueue manages a queue of expiration events.
// Deduplication is handled one layer up by the TimingWheelManager (one item
// per key with a version guard); this queue only needs to deliver events
// safely. mu serializes push against close so a timer goroutine can never
// send on a channel that is concurrently being closed.
type expirationQueue struct {
	events chan *ExpirationEvent
	closed atomic.Bool
	mu     sync.Mutex
}

// newExpirationQueue creates a new expiration queue with the specified buffer size.
func newExpirationQueue(bufferSize int) *expirationQueue {
	return &expirationQueue{
		events: make(chan *ExpirationEvent, bufferSize),
	}
}

// push adds an expiration event to the queue.
// Returns true if the event was added, false if queue is full or closed.
func (eq *expirationQueue) push(event *ExpirationEvent) bool {
	if eq.closed.Load() {
		return false
	}

	eq.mu.Lock()
	defer eq.mu.Unlock()

	// Re-check under the lock: close() may have run after the fast-path load.
	if eq.closed.Load() {
		return false
	}

	select {
	case eq.events <- event:
		return true
	default:
		return false
	}
}

// close closes the expiration queue.
func (eq *expirationQueue) close() {
	if eq.closed.CompareAndSwap(false, true) {
		eq.mu.Lock()
		defer eq.mu.Unlock()
		close(eq.events)
	}
}
