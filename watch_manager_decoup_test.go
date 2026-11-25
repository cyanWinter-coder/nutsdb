package nutsdb

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMessageWithTxID builds a message tagged with a transaction id so tests
// can assert per-transaction ordering guarantees.
func newMessageWithTxID(bucket, key string, txID uint64, seq int) *Message {
	msg := NewMessage(bucket, key, []byte(fmt.Sprintf("tx%d-val%d", txID, seq)), DataSetFlag, uint64(time.Now().UnixNano()))
	msg.TxID = txID
	msg.Seq = uint64(seq)
	return msg
}

// TestWatchManager_SlowSubscriberDoesNotBlockCommit verifies requirements 1 & 2:
// a subscriber that stops consuming must not block the commit path, gets
// disconnected with ErrSubscriberBacklogExceeded once its backlog passes the
// limit, and other subscribers keep receiving events.
func TestWatchManager_SlowSubscriberDoesNotBlockCommit(t *testing.T) {
	// Small, deterministic buffers for the test.
	oldReceiveBuf := receiveChanBufferSize
	oldThreshold := deadMessageThreshold
	receiveChanBufferSize = 8
	deadMessageThreshold = 16
	t.Cleanup(func() {
		receiveChanBufferSize = oldReceiveBuf
		deadMessageThreshold = oldThreshold
	})

	wm := startDistributor(t)
	t.Cleanup(func() { _ = wm.Stop(time.Second) })

	bucket := "slow_sub_bucket"
	slowKey := "slow_sub_key"
	fastKey := "fast_sub_key"

	// slow: never reads from its channel.
	slowSub, err := wm.subscribe(bucket, slowKey)
	require.NoError(t, err)

	// fast: a healthy subscriber on a different key; the test itself reads
	// its channel so nothing competes for the probe message.
	fastSub, err := wm.subscribe(bucket, fastKey)
	require.NoError(t, err)

	// The commit path only enqueues onto the hub channel; measure that a
	// burst far larger than the subscriber buffer completes promptly even
	// though one subscriber is permanently stuck. The burst stays within the
	// hub/pipeline capacity so the only backpressure exercised is the slow
	// subscriber's own channel.
	burst := receiveChanBufferSize + deadMessageThreshold + 4
	doneSending := make(chan struct{})
	start := time.Now()

	go func() {
		defer close(doneSending)
		for i := 0; i < burst; i++ {
			msg := NewMessage(bucket, slowKey, []byte(fmt.Sprintf("v%d", i)), DataSetFlag, uint64(time.Now().UnixNano()))
			// A full hub channel is allowed to return an error, but must
			// never block for a meaningful amount of time.
			_ = wm.sendMessage(msg)
		}
	}()

	select {
	case <-doneSending:
	case <-time.After(5 * time.Second):
		t.Fatal("commit path blocked while a subscriber was slow")
	}
	assert.Less(t, time.Since(start), 2*time.Second, "enqueuing the burst took too long")

	// Wait for the slow subscriber to be evicted.
	deadline := time.After(5 * time.Second)
	for {
		wm.mu.Lock()
		_, stillThere := wm.lookup[bucket][slowKey][slowSub.id]
		wm.mu.Unlock()
		if !stillThere {
			break
		}
		select {
		case <-deadline:
			t.Fatal("slow subscriber was not disconnected after exceeding the backlog limit")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// The slow channel must be closed and carry the identifiable reason.
	for {
		_, ok := <-slowSub.receiveChan
		if !ok {
			break
		}
	}
	assert.ErrorIs(t, slowSub.disconnectReason(), ErrSubscriberBacklogExceeded)
	assert.False(t, slowSub.active.Load())

	// The fast subscriber must survive and keep getting events. The hub send
	// is non-blocking, so retry the probe briefly if the collector has not
	// fully caught up yet.
	wmVerifySubscriberExists(t, wm, bucket, fastKey, fastSub.id)
	require.Eventually(t, func() bool {
		probe := NewMessage(bucket, fastKey, []byte("after-eviction"), DataSetFlag, uint64(time.Now().UnixNano()))
		return wm.sendMessage(probe) == nil
	}, 5*time.Second, 5*time.Millisecond, "probe event could not be enqueued")

	select {
	case msg, ok := <-fastSub.receiveChan:
		require.True(t, ok, "fast subscriber channel should remain open")
		assert.Equal(t, []byte("after-eviction"), msg.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("fast subscriber stopped receiving after the slow one was evicted")
	}
}

// TestWatchManager_StopDrainsAndClosesChannels verifies requirement 3:
// after Stop, no new events are accepted, events already enqueued are
// delivered best-effort within the timeout, and subscriber channels are
// closed.
func TestWatchManager_StopDrainsAndClosesChannels(t *testing.T) {
	wm := startDistributor(t)

	bucket := "drain_bucket"
	key := "drain_key"
	sub, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	queued := 50
	for i := 0; i < queued; i++ {
		wmSendMessage(t, wm, bucket, key, []byte(fmt.Sprintf("v%d", i)))
	}

	// Stop must reject further events and wait for the drain.
	require.NoError(t, wm.Stop(5*time.Second))
	assert.True(t, wm.isClosed())

	// New events are refused with the closed-manager error.
	err = wm.sendMessage(NewMessage(bucket, key, []byte("late"), DataSetFlag, uint64(time.Now().UnixNano())))
	assert.ErrorIs(t, err, ErrWatchManagerClosed)

	// Everything delivered before the close is observed; the channel then
	// closes exactly once.
	received := 0
	var channelClosed bool
	for !channelClosed {
		select {
		case _, ok := <-sub.receiveChan:
			if !ok {
				channelClosed = true
			} else {
				received++
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("channel never closed; received %d/%d", received, queued)
		}
	}
	assert.Equal(t, queued, received, "all enqueued events should be delivered before close")
}

// TestWatchManager_StopTimeoutDoesNotHang verifies requirement 3's timeout
// path: when internal workers cannot finish draining within the timeout, Stop
// still returns promptly instead of hanging the database shutdown, while
// leaving workers to wind down in the background (channels closed once, no
// panic).
func TestWatchManager_StopTimeoutDoesNotHang(t *testing.T) {
	oldReceiveBuf := receiveChanBufferSize
	oldThreshold := deadMessageThreshold
	receiveChanBufferSize = 4
	deadMessageThreshold = 1 << 30 // effectively disable eviction for this test
	t.Cleanup(func() {
		receiveChanBufferSize = oldReceiveBuf
		deadMessageThreshold = oldThreshold
	})

	wm := startDistributor(t)

	bucket := "timeout_bucket"
	key := "timeout_key"
	stuckSub, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	// Saturate the stuck subscriber's channel so queued events remain
	// undelivered at shutdown.
	for i := 0; i < receiveChanBufferSize+32; i++ {
		_ = wm.sendMessage(NewMessage(bucket, key, []byte(fmt.Sprintf("v%d", i)), DataSetFlag, uint64(time.Now().UnixNano())))
	}

	// Simulate a background worker that ignores the shutdown signal past the
	// Stop timeout (the real-world analogue of a wedged drain), then releases
	// shortly afterwards so no goroutine leaks out of the test.
	workerReleased := make(chan struct{})
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		<-workerReleased
	}()

	stopTimeout := 200 * time.Millisecond
	start := time.Now()
	err = wm.Stop(stopTimeout)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrCloseWatchManagerTimeout)
	assert.GreaterOrEqual(t, elapsed, stopTimeout-50*time.Millisecond, "Stop should wait for the timeout")
	assert.Less(t, elapsed, 2*time.Second, "Stop must return near its timeout, never hang")
	assert.True(t, wm.isClosed())

	// Manager state is shut immediately even on timeout: no new events are
	// accepted and repeated Stop calls are idempotent.
	err = wm.sendMessage(NewMessage(bucket, key, []byte("late"), DataSetFlag, uint64(time.Now().UnixNano())))
	assert.ErrorIs(t, err, ErrWatchManagerClosed)
	assert.ErrorIs(t, wm.Stop(time.Second), ErrWatchManagerClosed)

	// Release the wedged worker and wait for full wind-down. The subscriber
	// channel must end up closed exactly once (cleanUpSubscribers runs in the
	// startDistributor deferred cleanup after the workers' wg reaches zero).
	close(workerReleased)
	require.Eventually(t, func() bool {
		select {
		case _, ok := <-stuckSub.receiveChan:
			return !ok
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "channel must eventually be closed")
}

// TestWatchManager_RepeatedUnsubscribeIsNoOp verifies requirement 4: after the
// first unsubscribe (or any disconnect), repeated unsubscribes are no-ops and
// the channel is closed exactly once without panicking.
func TestWatchManager_RepeatedUnsubscribeIsNoOp(t *testing.T) {
	wm := startDistributor(t)
	t.Cleanup(func() { _ = wm.Stop(time.Second) })

	bucket := "idempotent_bucket"
	key := "idempotent_key"

	sub, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	// First unsubscribe closes the channel and removes the subscription.
	require.NoError(t, wm.unsubscribe(bucket, key, sub.id))
	_, ok := <-sub.receiveChan
	assert.False(t, ok, "channel should be closed after unsubscribe")

	// Repeated unsubscribes are no-ops returning nil.
	for i := 0; i < 5; i++ {
		assert.NoError(t, wm.unsubscribe(bucket, key, sub.id), "repeated unsubscribe #%d", i)
	}

	// Concurrent repeated unsubscribes must not panic on a double close.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, wm.unsubscribe(bucket, key, sub.id))
		}()
	}
	wg.Wait()

	// Unsubscribing an id that never existed is also a quiet no-op.
	assert.NoError(t, wm.unsubscribe(bucket, key, 999999))
}

// TestWatchManager_RepeatedCloseIsIdempotent verifies Stop/close can be called
// repeatedly and subscriber channels are closed a single time.
func TestWatchManager_RepeatedCloseIsIdempotent(t *testing.T) {
	wm := startDistributor(t)

	sub, err := wm.subscribe("close_bucket", "close_key")
	require.NoError(t, err)

	require.NoError(t, wm.Stop(2*time.Second))

	for i := 0; i < 3; i++ {
		assert.ErrorIs(t, wm.Stop(2*time.Second), ErrWatchManagerClosed)
		assert.ErrorIs(t, wm.close(), ErrWatchManagerClosed)
	}

	_, ok := <-sub.receiveChan
	assert.False(t, ok)

	// Unsubscribe after shutdown is a no-op.
	assert.NoError(t, wm.unsubscribe("close_bucket", "close_key", sub.id))
}

// TestWatchManager_EventOrderingAndTransactionContiguity verifies
// requirement 5: events reach each subscriber in commit order, and all events
// of one transaction are delivered consecutively with no interleaving.
func TestWatchManager_EventOrderingAndTransactionContiguity(t *testing.T) {
	wm := startDistributor(t)
	t.Cleanup(func() { _ = wm.Stop(time.Second) })

	bucket := "order_bucket"
	key := "order_key"
	sub, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	type sentEvent struct {
		txID uint64
		seq  int
	}

	const txCount = 20
	const eventsPerTx = 5
	var sent []sentEvent

	// Emit transactions one after another; within a transaction the events
	// are contiguous on the hub channel.
	for tx := uint64(1); tx <= txCount; tx++ {
		for seq := 0; seq < eventsPerTx; seq++ {
			require.NoError(t, wm.sendMessage(newMessageWithTxID(bucket, key, tx, seq)))
			sent = append(sent, sentEvent{txID: tx, seq: seq})
		}
	}

	// Read everything until a quiet period, then stop the manager to close
	// the channel. The quiet timer is refreshed on every message.
	var received []*Message
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		quiet := time.After(200 * time.Millisecond)
		for {
			select {
			case msg, ok := <-sub.receiveChan:
				if !ok {
					return
				}
				received = append(received, msg)
				quiet = time.After(200 * time.Millisecond)
			case <-quiet:
				return
			}
		}
	}()

	select {
	case <-drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not settle")
	}

	require.Len(t, received, len(sent), "every committed event should be delivered")

	// Global order must match the enqueue order exactly.
	for i, want := range sent {
		assert.Equal(t, want.txID, received[i].TxID, "txID mismatch at position %d", i)
		assert.Equal(t, uint64(want.seq), received[i].Seq, "seq mismatch at position %d", i)
	}

	// Re-walk and assert each transaction's events form one contiguous run.
	seenTx := make(map[uint64]bool)
	var lastTx uint64
	runLen := 0
	for _, msg := range received {
		if msg.TxID != lastTx {
			if lastTx != 0 {
				assert.Equal(t, eventsPerTx, runLen, "transaction %d was not delivered contiguously", lastTx)
			}
			assert.False(t, seenTx[msg.TxID], "transaction %d interleaved with another transaction", msg.TxID)
			seenTx[msg.TxID] = true
			lastTx = msg.TxID
			runLen = 0
		}
		runLen++
	}
	assert.Equal(t, eventsPerTx, runLen, "final transaction was not delivered contiguously")
}

// TestWatchManager_SlowSubscriberIsolationAcrossKeys verifies requirement 2
// across independent keys: evicting a stuck subscriber on one key leaves a
// healthy subscriber on another key completely unaffected.
func TestWatchManager_SlowSubscriberIsolationAcrossKeys(t *testing.T) {
	oldReceiveBuf := receiveChanBufferSize
	oldThreshold := deadMessageThreshold
	receiveChanBufferSize = 4
	deadMessageThreshold = 8
	t.Cleanup(func() {
		receiveChanBufferSize = oldReceiveBuf
		deadMessageThreshold = oldThreshold
	})

	wm := startDistributor(t)
	t.Cleanup(func() { _ = wm.Stop(time.Second) })

	bucket := "isolation_bucket"

	stuckSub, err := wm.subscribe(bucket, "stuck")
	require.NoError(t, err)
	healthySub, err := wm.subscribe(bucket, "healthy")
	require.NoError(t, err)

	// Overwhelm the stuck key.
	for i := 0; i < receiveChanBufferSize+deadMessageThreshold+8; i++ {
		_ = wm.sendMessage(NewMessage(bucket, "stuck", []byte("x"), DataSetFlag, uint64(time.Now().UnixNano())))
	}

	deadline := time.After(5 * time.Second)
	for {
		wm.mu.Lock()
		_, stillThere := wm.lookup[bucket]["stuck"][stuckSub.id]
		wm.mu.Unlock()
		if !stillThere {
			break
		}
		select {
		case <-deadline:
			t.Fatal("stuck subscriber was not evicted")
		case <-time.After(10 * time.Millisecond):
		}
	}
	assert.ErrorIs(t, stuckSub.disconnectReason(), ErrSubscriberBacklogExceeded)

	// The healthy key's subscription and channel are fully intact.
	wmVerifySubscriberExists(t, wm, bucket, "healthy", healthySub.id)
	wmSendMessage(t, wm, bucket, "healthy", []byte("alive"))
	msg := wmReceiveMessage(t, healthySub.receiveChan, bucket, "healthy", 2*time.Second)
	assert.Equal(t, []byte("alive"), msg.Value)
}

// TestDB_WatchCloseDoesNotHang reproduces the reported production symptom:
// with many watchers attached (some of them consuming slowly) and concurrent
// writes in flight, closing the database must return promptly instead of
// hanging until the process is killed.
func TestDB_WatchCloseDoesNotHang(t *testing.T) {
	opts := DefaultOptions
	opts.EnableWatch = true
	opts.Dir = filepath.Join(t.TempDir(), "watch-close-no-hang")

	db, err := Open(opts)
	require.NoError(t, err)

	bucket := "hang_bucket"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	const watcherCount = 20
	var watcherWg sync.WaitGroup

	// releaseSlow unblocks any slow callback; closed after Close so the
	// watcher goroutines can drain their in-flight batch and terminate.
	releaseSlow := make(chan struct{})

	// A mix of fast and deliberately slow watchers. The slow ones block on
	// releaseSlow while holding a callback, exercising the backlog/eviction
	// path while Close runs.
	for i := 0; i < watcherCount; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		slow := i%3 == 0

		watcher, errWatch := db.Watch(context.Background(), bucket, key, func(msg *Message) error {
			if slow {
				select {
				case <-releaseSlow:
				case <-time.After(500 * time.Millisecond):
				}
			}
			return nil
		})
		require.NoError(t, errWatch)

		watcherWg.Add(1)
		go func() {
			defer watcherWg.Done()
			_ = watcher.Run()
		}()
		require.NoError(t, watcher.WaitReady(5*time.Second))
	}

	// Concurrent writers keep producing events while the watchers exist.
	stopWrites := make(chan struct{})
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 0; ; i++ {
			select {
			case <-stopWrites:
				return
			default:
			}
			key := []byte(fmt.Sprintf("key-%d", i%watcherCount))
			_ = db.Update(func(tx *Tx) error {
				return tx.Put(bucket, key, testutils.GetRandomBytes(16), Persistent)
			})
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Close must complete within the status manager shutdown budget even
	// though slow watcher callbacks are still blocked and writers are active.
	// This is the core regression assertion for the reported hang.
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(15 * time.Second):
		t.Fatal("db.Close() hung with active watchers and concurrent writes")
	}
	close(stopWrites)
	writerWg.Wait()

	// Manager is shut; now release any blocked callbacks so the watcher
	// goroutines can finish draining and terminate.
	close(releaseSlow)

	done := make(chan struct{})
	go func() {
		watcherWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watcher goroutines did not exit after Close")
	}
}

// TestDB_WatchWritesNotBlockedBySlowSubscriber verifies at the database level
// that write latency stays bounded when a watcher stops consuming.
func TestDB_WatchWritesNotBlockedBySlowSubscriber(t *testing.T) {
	opts := DefaultOptions
	opts.EnableWatch = true
	opts.Dir = filepath.Join(t.TempDir(), "watch-write-decoupled")

	db, err := Open(opts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	bucket := "decoupled_bucket"
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)

	// A watcher whose callback blocks forever, so its receive channel fills.
	block := make(chan struct{})
	watcher, errWatch := db.Watch(context.Background(), bucket, []byte("watched"), func(msg *Message) error {
		<-block
		return nil
	})
	require.NoError(t, errWatch)
	go func() { _ = watcher.Run() }()
	require.NoError(t, watcher.WaitReady(5*time.Second))
	t.Cleanup(func() { close(block) })

	// Many writes to the watched key while the watcher is stuck. Each
	// transaction must still complete promptly; the watch layer can never
	// hold up the commit path.
	const writes = 200
	start := time.Now()
	for i := 0; i < writes; i++ {
		done := make(chan error, 1)
		go func(i int) {
			done <- db.Update(func(tx *Tx) error {
				return tx.Put(bucket, []byte("watched"), []byte(fmt.Sprintf("v%d", i)), Persistent)
			})
		}(i)
		select {
		case requireErr := <-done:
			require.NoError(t, requireErr)
		case <-time.After(3 * time.Second):
			t.Fatalf("write %d blocked by the slow watcher", i)
		}
	}
	assert.Less(t, time.Since(start), 15*time.Second, "writes took too long while a watcher was stuck")
}
