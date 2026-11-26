package nutsdb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/nutsdb/nutsdb/internal/core"
)

// ErrBucketNotExist is kept for compatibility with older callers. It is the
// same sentinel as ErrBucketNotFound so errors.Is works across bucket APIs.
var ErrBucketNotExist = ErrBucketNotFound

// ErrBucketManagerClosed is returned when a closed bucket manager is used.
var ErrBucketManagerClosed = errors.New("bucket manager is closed")

const BucketStoreFileName = "bucket.Meta"

type BucketManager struct {
	// mu serializes metadata commits and recovery replacement. It also makes
	// direct BucketManager use safe under concurrent bucket creation.
	mu sync.RWMutex

	dir  string
	path string
	fd   *os.File

	// BucketInfoMapper BucketID => Bucket itself
	BucketInfoMapper core.InfoMapperInBucket
	BucketIDMarker   core.IDMarkerInBucket
	// IDGenerator helps generates an ID for every single bucket
	Gen *IDGenerator

	closed bool
}

func NewBucketManager(dir string) (*BucketManager, error) {
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}

	bucketFilePath := filepath.Join(dir, BucketStoreFileName)
	fd, err := openBucketMetaFile(bucketFilePath)
	if err != nil {
		return nil, err
	}

	bm := &BucketManager{
		dir:              dir,
		path:             bucketFilePath,
		fd:               fd,
		BucketInfoMapper: make(core.InfoMapperInBucket),
		BucketIDMarker:   make(core.IDMarkerInBucket),
		Gen:              &IDGenerator{currentMaxId: 0},
	}
	return bm, nil
}

func openBucketMetaFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
}

type bucketSubmitRequest struct {
	ds     core.Ds
	name   core.BucketName
	bucket *core.Bucket
}

// SubmitPendingBucketChange durably appends bucket metadata and only then
// publishes the corresponding in-memory directory change. If the append or
// sync fails, the on-disk prefix is rolled back and the current in-memory
// maps remain untouched.
func (bm *BucketManager) SubmitPendingBucketChange(reqs []*bucketSubmitRequest) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.fd == nil {
		return ErrBucketManagerClosed
	}

	nextInfo, nextMarker, err := bm.cloneStateLocked()
	if err != nil {
		return err
	}

	encoded := make([]byte, 0)
	for _, req := range reqs {
		if req == nil || req.bucket == nil {
			return errors.New("bucket change request is nil")
		}
		// Commit-time compare-and-set closes the check-then-act race for
		// callers that do not hold the database write lock.
		switch {
		case req.bucket.Meta != nil && req.bucket.Meta.Op == core.BucketInsertOperation:
			if marker, ok := nextMarker[req.bucket.Name]; ok {
				if id, ok := marker[req.bucket.Ds]; ok {
					if _, alive := nextInfo[id]; alive {
						return ErrBucketAlreadyExist
					}
				}
			}
		case req.bucket.Meta != nil && req.bucket.Meta.Op == core.BucketDeleteOperation:
			marker, ok := nextMarker[req.bucket.Name]
			if !ok {
				return ErrBucketNotExist
			}
			id, ok := marker[req.bucket.Ds]
			if !ok {
				return ErrBucketNotExist
			}
			if _, alive := nextInfo[id]; !alive {
				return ErrBucketNotExist
			}
		}

		encoded = append(encoded, req.bucket.Encode()...)
		bm.Gen.CompareAndSetMaxId(req.bucket.Id)
		applyBucketChange(nextInfo, nextMarker, req.bucket)
	}

	startOffset, err := bm.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	rollbackFile := func() {
		_ = bm.fd.Truncate(startOffset)
		_, _ = bm.fd.Seek(startOffset, io.SeekStart)
	}

	if len(encoded) > 0 {
		n, err := bm.fd.Write(encoded)
		if err != nil {
			rollbackFile()
			return err
		}
		if n != len(encoded) {
			rollbackFile()
			return io.ErrShortWrite
		}
		if err := bm.fd.Sync(); err != nil {
			rollbackFile()
			return err
		}
	}

	// Publish the prepared directory in one assignment.
	bm.BucketInfoMapper = nextInfo
	bm.BucketIDMarker = nextMarker
	return nil
}

func (bm *BucketManager) cloneStateLocked() (core.InfoMapperInBucket, core.IDMarkerInBucket, error) {
	nextInfo := make(core.InfoMapperInBucket, len(bm.BucketInfoMapper))
	for id, bucket := range bm.BucketInfoMapper {
		nextInfo[id] = bucket
	}

	nextMarker := make(core.IDMarkerInBucket, len(bm.BucketIDMarker))
	for name, dsToID := range bm.BucketIDMarker {
		copied := make(map[core.Ds]core.BucketId, len(dsToID))
		for ds, id := range dsToID {
			copied[ds] = id
		}
		nextMarker[name] = copied
	}

	if bm.Gen == nil {
		return nil, nil, ErrBucketManagerClosed
	}
	return nextInfo, nextMarker, nil
}

func applyBucketChange(info core.InfoMapperInBucket, marker core.IDMarkerInBucket, bucket *core.Bucket) {
	if bucket.Meta == nil {
		bucket.Meta = &core.BucketMeta{}
	}

	switch bucket.Meta.Op {
	case core.BucketInsertOperation, core.BucketUpdateOperation:
		// If recovery sees a duplicate insert for the same ds/name, the
		// latest record supersedes the previous ID. Drop that stale ID so
		// it does not remain as an orphan directory entry.
		if dsToID, ok := marker[bucket.Name]; ok {
			if oldID, exists := dsToID[bucket.Ds]; exists && oldID != bucket.Id {
				delete(info, oldID)
			}
		}
		info[bucket.Id] = bucket
		if _, exist := marker[bucket.Name]; !exist {
			marker[bucket.Name] = make(map[core.Ds]core.BucketId)
		}
		marker[bucket.Name][bucket.Ds] = bucket.Id

	case core.BucketDeleteOperation:
		// Remove every duplicate directory entry for this exact
		// (data structure, name), not only the ID carried by the tombstone.
		for id, candidate := range info {
			if candidate != nil && candidate.Ds == bucket.Ds && candidate.Name == bucket.Name {
				delete(info, id)
			}
		}

		if dsToID, exist := marker[bucket.Name]; exist {
			delete(dsToID, bucket.Ds)
			if len(dsToID) == 0 {
				delete(marker, bucket.Name)
			} else {
				marker[bucket.Name] = dsToID
			}
		}
	}
}

// ReplaceRecoveredBuckets replays metadata records in commit order. Keeping
// the append-only file untouched is important: rewriting it on open would
// both change its committed size and risk corrupting the log if interrupted.
// In-memory application deduplicates a ds/name pair so the last successfully
// committed record wins and superseded IDs are removed as orphans.
func (bm *BucketManager) ReplaceRecoveredBuckets(records []*core.Bucket) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.Gen == nil {
		bm.Gen = &IDGenerator{}
	}
	if bm.BucketInfoMapper == nil {
		bm.BucketInfoMapper = make(core.InfoMapperInBucket)
	}
	if bm.BucketIDMarker == nil {
		bm.BucketIDMarker = make(core.IDMarkerInBucket)
	}
	for _, bucket := range records {
		if bucket == nil {
			continue
		}
		bm.Gen.CompareAndSetMaxId(bucket.Id)
		applyBucketChange(bm.BucketInfoMapper, bm.BucketIDMarker, bucket)
	}
	return nil
}

// RecoverBucket replays one metadata record during recovery.
func (bm *BucketManager) RecoverBucket(bucket *core.Bucket) {
	if bucket == nil {
		return
	}
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.Gen == nil {
		bm.Gen = &IDGenerator{}
	}
	if bm.BucketInfoMapper == nil {
		bm.BucketInfoMapper = make(core.InfoMapperInBucket)
	}
	if bm.BucketIDMarker == nil {
		bm.BucketIDMarker = make(core.IDMarkerInBucket)
	}
	bm.Gen.CompareAndSetMaxId(bucket.Id)
	applyBucketChange(bm.BucketInfoMapper, bm.BucketIDMarker, bucket)
}

// BucketIDsByName returns every known ID for a ds/name pair, including stale
// duplicate directory entries in a not-yet-restarted process.
func (bm *BucketManager) BucketIDsByName(ds core.Ds, name core.BucketName) []core.BucketId {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	ids := make([]core.BucketId, 0, 1)
	if dsToID, ok := bm.BucketIDMarker[name]; ok {
		if id, ok := dsToID[ds]; ok {
			ids = append(ids, id)
		}
	}
	for id, bucket := range bm.BucketInfoMapper {
		if bucket != nil && bucket.Ds == ds && bucket.Name == name {
			found := false
			for _, existing := range ids {
				if existing == id {
					found = true
					break
				}
			}
			if !found {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

type IDGenerator struct {
	mu           sync.Mutex
	currentMaxId uint64
}

func (g *IDGenerator) GenId() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.currentMaxId++
	return g.currentMaxId
}

func (g *IDGenerator) CompareAndSetMaxId(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if id > g.currentMaxId {
		g.currentMaxId = id
	}
}

func (bm *BucketManager) ExistBucket(ds core.Ds, name core.BucketName) bool {
	bucket, err := bm.GetBucket(ds, name)
	return bucket != nil && err == nil
}

func (bm *BucketManager) GetBucket(ds core.Ds, name core.BucketName) (b *core.Bucket, err error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	ds2IdMapper := bm.BucketIDMarker[name]
	if ds2IdMapper == nil {
		return nil, ErrBucketNotExist
	}

	if id, exist := ds2IdMapper[ds]; exist {
		if bucket, ok := bm.BucketInfoMapper[id]; ok {
			return bucket, nil
		}
		return nil, ErrBucketNotExist
	}
	return nil, ErrBucketNotExist
}

func (bm *BucketManager) GetBucketById(id core.BucketId) (*core.Bucket, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	if bucket, exist := bm.BucketInfoMapper[id]; exist {
		return bucket, nil
	}
	return nil, ErrBucketNotExist
}

func (bm *BucketManager) GetBucketID(ds core.Ds, name core.BucketName) (core.BucketId, error) {
	if bucket, err := bm.GetBucket(ds, name); err != nil {
		return 0, err
	} else {
		return bucket.Id, nil
	}
}

func (bm *BucketManager) Close() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.closed {
		return nil
	}
	bm.closed = true
	if bm.fd == nil {
		return nil
	}
	err := bm.fd.Close()
	bm.fd = nil
	return err
}
