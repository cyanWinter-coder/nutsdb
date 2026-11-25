package nutsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/utils"
)

// errors
var (
	ErrBucketSubscriberNotFound = errors.New("bucket subscriber not found")
	ErrKeySubscriberNotFound    = errors.New("key subscriber not found")
	ErrSubscriberNotFound       = errors.New("subscriber not found")
	ErrWatchChanCannotSend      = errors.New("watch channel cannot send")
	ErrKeyAlreadySubscribed     = errors.New("key already subscribed")
	ErrWatchManagerClosed       = errors.New("watch manager closed")
	ErrWatchingCallbackFailed   = errors.New("watching callback failed")
	ErrWatchingChannelClosed    = errors.New("watching channel closed")
	ErrChannelNotAvailable      = errors.New("channel not available")
	ErrCloseWatchManagerTimeout = errors.New("close watch manager timeout")
	// ErrSubscriberBacklogExceeded is returned (and recorded on the
	// subscriber) when a subscriber cannot keep up with the event stream and
	// its backlog grows past the configured limit. The offending subscription
	// is disconnected; other subscriptions are not affected.
	ErrSubscriberBacklogExceeded = errors.New("subscriber backlog exceeded limit")
)

// convert these variables to var for testing
var (
	watchChanBufferSize      = 1024
	receiveChanBufferSize    = 1024
	maxBatchSize             = 1024
	deadMessageThreshold     = 100
	distributeChanBufferSize = 128
	victimBucketBufferSize   = 128
)

const (
	DefaultCallbackTimeout = 1 * time.Second
	// defaultCloseDrainTimeout bounds how long the watch manager keeps
	// flushing already-enqueued events to subscribers during shutdown.
	defaultCloseDrainTimeout = 5 * time.Second
)

// message priority
const (
	MessagePriorityHigh   = iota // the messages must be ensured to deliver to distributor
	MessagePriorityMedium = 1    // the messages may be dropped
)

type (
	bucketToSubscribers       map[core.BucketName]map[string]map[uint64]*subscriber
	victimBucketToSubscribers map[uint64]map[string]map[uint64]*subscriber
	victimBucketChan          chan victimBucketToSubscribers
	MessagePriority           int

	WatchOptions struct {
		CallbackTimeout time.Duration
	}

	MessageOptions struct {
		Priority MessagePriority
	}
)

type Message struct {
	BucketName core.BucketName
	Key        string
	Value      []byte
	Flag       DataFlag
	Timestamp  uint64
	// TxID identifies the committing transaction. All messages sharing the
	// same TxID are delivered consecutively and in commit order.
	TxID uint64
	// Seq is the position of the message within its committing transaction
	// (starting at 0), allowing consumers to verify delivery order.
	Seq uint64

	priority MessagePriority
}

func NewMessage(bucketName core.BucketName, key string, value []byte, flag DataFlag, timestamp uint64, options ...MessageOptions) *Message {
	var priority MessagePriority
	// default priority is medium
	priority = MessagePriorityMedium

	if len(options) > 0 {
		priority = options[0].Priority
	}

	return &Message{
		BucketName: bucketName,
		Key:        key,
		Value:      value,
		Flag:       flag,
		Timestamp:  timestamp,
		priority:   priority,
	}
}

func NewWatchOptions() *WatchOptions {
	return &WatchOptions{
		CallbackTimeout: DefaultCallbackTimeout,
	}
}

// WithCallbackTimeout sets the callback timeout
func (opts *WatchOptions) WithCallbackTimeout(timeout time.Duration) {
	opts.CallbackTimeout = timeout
}

type subscriber struct {
	id          uint64
	bucketName  core.BucketName
	key         string
	receiveChan chan *Message
	// deadMessages counts consecutive events that could not be pushed into
	// receiveChan because the subscriber was not consuming them.
	deadMessages int
	active       atomic.Bool

	// closeOnce guarantees the receive channel is closed exactly once no
	// matter how many paths (unsubscribe, slow-consumer eviction, bucket
	// deletion, manager shutdown) race to disconnect the subscriber.
	closeOnce sync.Once
	// disconnectErr records why the channel was closed so consumers can
	// distinguish a backlog eviction from an ordinary unsubscribe/shutdown.
	disconnectErr atomic.Value // error
}

// closeChannel closes the subscriber's receive channel at most once and marks
// the subscriber inactive. It is safe to call concurrently from any goroutine.
func (s *subscriber) closeChannel(err error) {
	s.closeOnce.Do(func() {
		if err != nil {
			s.disconnectErr.Store(err)
		}
		s.active.Store(false)
		close(s.receiveChan)
	})
}

// disconnectReason returns the error that caused the channel to close, or
// ErrWatchingChannelClosed for an ordinary shutdown/unsubscribe.
func (s *subscriber) disconnectReason() error {
	if err, ok := s.disconnectErr.Load().(error); ok && err != nil {
		return err
	}
	return ErrWatchingChannelClosed
}

type watchManager struct {
	lookup         bucketToSubscribers // bucketName -> key -> id -> subscriber
	watchChan      chan *Message       // the hub channel to receive the messages; never closed
	distributeChan chan []*Message     // the collector worker collects messages from watchChan and sends them to the distributor worker through distributeChan
	victimMaps     victimBucketToSubscribers
	victimChan     victimBucketChan
	workerCtx      context.Context // cancellation for in-flight tasks
	workerCancel   context.CancelFunc
	wg             sync.WaitGroup

	closed      bool
	started     bool // indicates whether Start() has been called
	idGenerator *IDGenerator

	muClosed  sync.RWMutex
	muStarted sync.RWMutex
	mu        sync.Mutex
}

func NewWatchManager() *watchManager {
	ctx := context.Background()
	workerCtx, workerCancel := context.WithCancel(ctx)
	return &watchManager{
		lookup:         make(bucketToSubscribers),
		watchChan:      make(chan *Message, watchChanBufferSize),
		distributeChan: make(chan []*Message, distributeChanBufferSize),
		closed:         false,
		started:        false,
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
		idGenerator:    &IDGenerator{currentMaxId: 0},
		victimMaps:     make(victimBucketToSubscribers),
		victimChan:     make(victimBucketChan, victimBucketBufferSize),
	}
}

// Name returns the component name
func (wm *watchManager) Name() string {
	return "WatchManager"
}

// sendMessage enqueues a message without ever blocking on subscriber
// consumption. A full hub channel means the write path is not keeping up with
// the collector; the event is rejected with an identifiable error instead of
// blocking the committing transaction.
func (wm *watchManager) sendMessage(message *Message) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	select {
	case <-wm.workerCtx.Done():
		return ErrWatchManagerClosed
	default:
	}

	select {
	case wm.watchChan <- message:
		return nil
	default:
		if message.priority == MessagePriorityHigh {
			// High-priority events (e.g. bucket deletion) are allowed to wait
			// for hub capacity, but shutdown always wins and the wait never
			// blocks a closing database.
			select {
			case wm.watchChan <- message:
				utils.GetLogger().Printf("[watch_manager] Sent high priority message %s/%s to watch channel\n", message.BucketName, message.Key)
				return nil
			case <-wm.workerCtx.Done():
				return ErrWatchManagerClosed
			}
		}
		return ErrWatchChanCannotSend
	}
}

func (wm *watchManager) sendUpdatedEntries(entries []*core.Entry, deletedbuckets map[core.BucketName]bool, getBucketName func(bucketId core.BucketId) (core.BucketName, error)) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	// send all updated entries to the watch manager; entries are committed in
	// slice order, which also fixes their delivery order.
	if len(entries) > 0 {
		seqByTx := make(map[uint64]uint64)
		for _, entry := range entries {
			bucketName, err := getBucketName(entry.Meta.BucketId)
			if err != nil {
				continue
			}

			rawKey, err := entry.GetRawKey()
			if err != nil {
				utils.GetLogger().Printf("get raw key %+v error: %+v", entry.Key, err)
				continue
			}

			message := NewMessage(bucketName, string(rawKey), entry.Value, entry.Meta.Flag, entry.Meta.Timestamp)
			message.TxID = entry.Meta.TxID
			message.Seq = seqByTx[entry.Meta.TxID]
			seqByTx[entry.Meta.TxID]++
			if err := wm.sendMessage(message); err != nil {
				return err
			}
		}
	}

	//
	for bucketName := range deletedbuckets {
		message := NewMessage(bucketName, "", nil, DataBucketDeleteFlag, uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		if err := wm.sendMessage(message); err != nil {
			return err
		}
	}

	return nil
}

// startDistributor starts both the collector and distributor goroutines
func (wm *watchManager) startDistributor() {
	defer wm.cleanUpSubscribers()

	// start the victim collector goroutine
	// it collects the victim buckets from the victim channel
	// and handle delete bucket operation
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runVictimCollector()
	}()

	// Start the distributor goroutine (consumes from distributeChan)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runDistributor()
	}()

	// start the collector goroutine (collects messages into batches)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runCollector()
	}()

	wm.wg.Wait()
}

// collectorFlushInterval bounds how long a partially filled batch waits in
// the collector before being handed to the distributor. A steady flush
// cadence keeps healthy subscriber channels draining instead of receiving
// giant spikes that look like a slow-consumer backlog.
var collectorFlushInterval = 5 * time.Millisecond

// runCollector collects messages from watchChan and batches them.
// watchChan is intentionally never closed: producers stop sending once the
// manager is marked closed, and the collector drains everything already
// enqueued before exiting, then closes distributeChan to signal the
// distributor.
func (wm *watchManager) runCollector() {
	batches := make([]*Message, 0, maxBatchSize)
	flushTicker := time.NewTicker(collectorFlushInterval)
	defer flushTicker.Stop()

	flush := func() {
		if len(batches) == 0 {
			return
		}
		wm.sendBatchToDistributor(batches)
		batches = batches[:0]
	}

	defer func() {
		// drain any events that arrived between the last batch flush and the
		// shutdown signal so already-enqueued events get a chance of delivery
		wm.drainWatchChan(&batches)
		if len(batches) > 0 {
			wm.deliverFinalBatch(batches)
		}
		close(wm.distributeChan)
	}()

	for {
		select {
		case msg := <-wm.watchChan:
			batches = append(batches, msg)
			if len(batches) >= maxBatchSize {
				flush()
			}
		case <-flushTicker.C:
			flush()
		case <-wm.workerCtx.Done():
			return
		}
	}
}

// drainWatchChan non-blockingly pulls every currently buffered hub message
// into the batch. Used during shutdown to salvage queued events.
func (wm *watchManager) drainWatchChan(batches *[]*Message) {
	for {
		select {
		case msg := <-wm.watchChan:
			*batches = append(*batches, msg)
		default:
			return
		}
	}
}

// deliverFinalBatch hands one last batch to the distributor during shutdown.
// It blocks until the distributor accepts the batch or the manager is fully
// torn down; Stop's timeout bounds how long callers wait overall.
func (wm *watchManager) deliverFinalBatch(batch []*Message) {
	sendBatch := make([]*Message, len(batch))
	copy(sendBatch, batch)

	select {
	case wm.distributeChan <- sendBatch:
	default:
		// Distributor may have already exited; do not block shutdown forever.
		utils.GetLogger().Printf("[watch_manager] Dropping final batch of %d messages during shutdown\n", len(sendBatch))
	}
}

// sendBatchToDistributor hands a batch to the distributor. Backpressure here
// is confined to the collector goroutine — commit-path producers only interact
// with the buffered watchChan — so blocking on capacity never stalls a
// transaction. Shutdown still wins via the worker context.
func (wm *watchManager) sendBatchToDistributor(batch []*Message) {
	sendBatch := make([]*Message, len(batch))
	copy(sendBatch, batch)

	select {
	case wm.distributeChan <- sendBatch:
	case <-wm.workerCtx.Done():
		// On shutdown make one last non-blocking attempt, otherwise the
		// batch is abandoned.
		select {
		case wm.distributeChan <- sendBatch:
		default:
			utils.GetLogger().Printf("[watch_manager] Dropping batch of %d messages during shutdown\n", len(sendBatch))
		}
	}
}

// runDistributor distributes batches to subscribers. It exits only after
// distributeChan is closed and fully drained, so events enqueued before
// shutdown are delivered as long as subscribers accept them.
func (wm *watchManager) runDistributor() {
	for batch := range wm.distributeChan {
		_ = wm.distributeAllMessages(batch)
	}
}

// runVictimCollector collects the victim buckets from the victim channel
// and handle delete bucket operation
// The bucket is deleted only when its all ds bucket are deleted
// we will send the delete bucket message to the subscribers when the bucket is deleted
func (wm *watchManager) runVictimCollector() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case victimBucketToSubscribers, ok := <-wm.victimChan:
			if !ok {
				return
			}

			wm.handleVictimBuckets(victimBucketToSubscribers)
		case <-ticker.C:
			// avoid busy spinning
		case <-wm.workerCtx.Done():
			// Shutdown: non-blockingly process everything already queued so
			// victim subscribers still get their delete notification.
			wm.drainVictimBuckets()
			return
		}
	}
}

// drainVictimBuckets processes every victim map already buffered without
// waiting for new ones.
func (wm *watchManager) drainVictimBuckets() {
	for {
		select {
		case victimBucketToSubscribers, ok := <-wm.victimChan:
			if !ok {
				return
			}
			wm.handleVictimBuckets(victimBucketToSubscribers)
		default:
			return
		}
	}
}

func (wm *watchManager) handleVictimBuckets(victimBucketToSubscribers victimBucketToSubscribers) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.handleVictimBucketsLocked(victimBucketToSubscribers)
}

// handleVictimBucketsLocked performs the victim processing. The caller must
// hold wm.mu (the distributor calls it from deleteBucket while already
// holding the lock).
func (wm *watchManager) handleVictimBucketsLocked(victimBucketToSubscribers victimBucketToSubscribers) {
	for identifierId, bucketMap := range victimBucketToSubscribers {
		for key, keyMap := range bucketMap {
			for _, sub := range keyMap {
				if sub.active.Load() {
					message := NewMessage(sub.bucketName, sub.key, nil, DataBucketDeleteFlag, uint64(time.Now().Unix()))
					select {
					case sub.receiveChan <- message:
					default:
						// drop the message; the channel close still follows
					}
					sub.closeChannel(nil)
				}
				delete(keyMap, sub.id)
			}
			delete(bucketMap, key)
		}
		delete(victimBucketToSubscribers, identifierId)
	}
}

// distribute the messages to the subscribers.
// The lock only guards the lookup table; every channel send is non-blocking,
// so a slow subscriber can never stall the distributor, the commit path or
// any other subscriber.
func (wm *watchManager) distributeAllMessages(messages []*Message) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		bucketMap, ok := wm.lookup[message.BucketName]
		if !ok {
			continue
		}

		if message.Flag == DataBucketDeleteFlag {
			// delete the bucket from the lookup
			wm.deleteBucket(*message)
			continue
		}

		key := message.Key
		subscriberMap, ok := bucketMap[key]
		if !ok {
			continue
		}

		// avoid blocking the distributor, all messages blocked will be dropped
		for _, sub := range subscriberMap {
			if !sub.active.Load() {
				utils.GetLogger().Printf("[watch_manager] Skipping inactive subscriber with id %d for message %s/%s\n", sub.id, message.BucketName, message.Key)
				continue
			}

			select {
			case sub.receiveChan <- message:
				sub.deadMessages = 0
			default:
				// the subscriber is not consuming; count the backlog and
				// disconnect only this subscriber once the limit is reached.
				sub.deadMessages++
				if sub.deadMessages >= deadMessageThreshold {
					wm.evictSubscriber(message, sub)
				}
			}
		}
	}

	return nil
}

// evictSubscriber removes a slow subscriber from the lookup and closes its
// channel exactly once with an identifiable backlog error. Must be called
// with wm.mu held.
func (wm *watchManager) evictSubscriber(message *Message, sub *subscriber) {
	utils.GetLogger().Printf("[watch_manager] Force-unsubscribing slow subscriber with id %d for message %s/%s after %d undeliverable messages\n",
		sub.id, message.BucketName, message.Key, sub.deadMessages)

	if _, err := wm.findSubscriber(message.BucketName, message.Key, sub.id); err == nil {
		delete(wm.lookup[message.BucketName][message.Key], sub.id)
		if len(wm.lookup[message.BucketName][message.Key]) == 0 {
			delete(wm.lookup[message.BucketName], message.Key)
		}
		if len(wm.lookup[message.BucketName]) == 0 {
			delete(wm.lookup, message.BucketName)
		}
	}

	sub.closeChannel(ErrSubscriberBacklogExceeded)
}

// subscribe to the key and bucket
// each subscriber has a own channel to receive messages
func (wm *watchManager) subscribe(bucketName core.BucketName, key string) (*subscriber, error) {
	if wm.isClosed() {
		return nil, ErrWatchManagerClosed
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()
	if _, ok := wm.lookup[bucketName]; !ok {
		wm.lookup[bucketName] = make(map[string]map[uint64]*subscriber)
	}

	receiveChan := make(chan *Message, receiveChanBufferSize)

	if _, ok := wm.lookup[bucketName][key]; !ok {
		wm.lookup[bucketName][key] = make(map[uint64]*subscriber)
	}

	id := wm.idGenerator.GenId()
	registeredSubscriber := subscriber{
		id:          id,
		bucketName:  bucketName,
		key:         key,
		receiveChan: receiveChan,
		active:      atomic.Bool{},
	}
	registeredSubscriber.active.Store(true)

	wm.lookup[bucketName][key][id] = &registeredSubscriber

	return &registeredSubscriber, nil
}

// unsubscribe removes a subscriber and closes its channel. It is idempotent:
// after the first call — including any eviction, bucket deletion or manager
// shutdown that already disconnected the subscriber — repeat calls are
// no-ops that return nil, and the channel is closed at most once.
func (wm *watchManager) unsubscribe(bucketName core.BucketName, key string, id core.BucketId) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	sub, err := wm.findSubscriber(bucketName, key, id)
	if err != nil {
		// Already removed (duplicate unsubscribe, eviction, bucket delete or
		// shutdown): nothing to do and nothing to report.
		return nil
	}

	// Clean up the subscriber
	delete(wm.lookup[bucketName][key], id)
	if len(wm.lookup[bucketName][key]) == 0 {
		delete(wm.lookup[bucketName], key)
	}
	if len(wm.lookup[bucketName]) == 0 {
		delete(wm.lookup, bucketName)
	}

	// closeChannel is a once-only operation.
	sub.closeChannel(nil)

	return nil
}

func (wm *watchManager) cleanUpSubscribers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for bucket, bucketMap := range wm.lookup {
		for key, keyMap := range bucketMap {
			for _, sub := range keyMap {
				sub.closeChannel(nil)
				delete(keyMap, sub.id)
			}
			delete(bucketMap, key)
		}
		delete(wm.lookup, bucket)
	}
}

// close marks the manager as shutting down and cancels the worker context.
// It never blocks: producers stop accepting new events immediately, while the
// worker goroutines drain already-enqueued events in the background. Use
// Stop to wait for that drain with a timeout.
// close is idempotent; repeat calls return ErrWatchManagerClosed.
func (wm *watchManager) close() error {
	wm.muClosed.Lock()
	defer wm.muClosed.Unlock()

	if wm.closed {
		return ErrWatchManagerClosed
	}

	wm.closed = true
	wm.workerCancel()

	return nil
}

func (wm *watchManager) findSubscriber(bucketName core.BucketName, key string, id uint64) (*subscriber, error) {
	if _, ok := wm.lookup[bucketName]; !ok {
		return nil, ErrBucketSubscriberNotFound
	}
	if _, ok := wm.lookup[bucketName][key]; !ok {
		return nil, ErrKeySubscriberNotFound
	}

	if sub, ok := wm.lookup[bucketName][key][id]; ok {
		return sub, nil
	}
	return nil, ErrSubscriberNotFound
}

func (wm *watchManager) done() <-chan struct{} {
	return wm.workerCtx.Done()
}

func (wm *watchManager) isClosed() bool {
	wm.muClosed.RLock()
	defer wm.muClosed.RUnlock()

	return wm.closed
}

/*
* delete the buckets from the watch manager
* and notify the subscribers that the keys are deleted due to deleted buckets
* @param deletedbuckets: the buckets to be deleted
 */
func (wm *watchManager) deleteBucket(deletingMessageBucket Message) {
	bucketName := deletingMessageBucket.BucketName

	if _, ok := wm.lookup[bucketName]; !ok {
		return
	}

	identifierId := wm.idGenerator.GenId()
	victimBucketToSubscribers := make(victimBucketToSubscribers)
	victimBucketToSubscribers[identifierId] = wm.lookup[bucketName]
	delete(wm.lookup, bucketName)

	// Log before sending to avoid race condition with victimCollector
	utils.GetLogger().Printf("[watch_manager] Moving bucket %s to victim channel (identifier: %d)\n", bucketName, identifierId)

	// Hand off without blocking the distributor (and therefore the commit
	// path). If the victim channel is full, close the victim subscribers
	// inline so their channels still terminate promptly.
	select {
	case wm.victimChan <- victimBucketToSubscribers:
		// Successfully sent - victimCollector now owns the map, don't access it anymore
	default:
		utils.GetLogger().Printf("[watch_manager] Victim channel full, closing bucket %s subscribers inline\n", bucketName)
		// deleteBucket is invoked from distributeAllMessages while wm.mu is
		// already held, so use the lock-free helper.
		wm.handleVictimBucketsLocked(victimBucketToSubscribers)
	}
}

// Start starts the watch manager
// Implements Component interface
func (wm *watchManager) Start(ctx context.Context) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	wm.muStarted.Lock()
	if wm.started {
		wm.muStarted.Unlock()
		return nil
	}

	wm.started = true
	wm.muStarted.Unlock()

	// use a local ready channel to wait for goroutine startup
	ready := make(chan struct{})

	go func() {
		close(ready) // signal that goroutine has started
		wm.startDistributor()
	}()

	// wait for distributor goroutine to start before returning
	select {
	case <-ready:
		utils.GetLogger().Printf("[watch_manager] Watch manager distributor started\n")
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for watch manager distributor to start")
	}
}

// Stop stops the watch manager.
// New events are rejected immediately; already-enqueued events are flushed to
// subscribers for up to timeout before all subscriber channels are closed.
// Implements Component interface.
func (wm *watchManager) Stop(timeout time.Duration) (err error) {
	if err := wm.close(); err != nil {
		// Idempotent: a second Stop reports the manager was already closed.
		return err
	}

	done := make(chan struct{})
	go func() {
		wm.wg.Wait()
		close(done)
	}()

	if timeout <= 0 {
		timeout = defaultCloseDrainTimeout
	}

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrCloseWatchManagerTimeout
	}
}
