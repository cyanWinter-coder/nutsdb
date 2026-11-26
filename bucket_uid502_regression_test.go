package nutsdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketUID502_DuplicateBucketCreate(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		const bucket = "bucket_dup"

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txCreateBucket(t, db, DataStructureBTree, bucket, ErrBucketAlreadyExist)

		// The same name under another data structure is a different bucket.
		txCreateBucket(t, db, DataStructureList, bucket, nil)
		txCreateBucket(t, db, DataStructureList, bucket, ErrBucketAlreadyExist)

		// Duplicate creation inside a single transaction is rejected too.
		err := db.Update(func(tx *Tx) error {
			AssertErr(t, tx.NewBucket(DataStructureSet, "bucket_dup_2"), nil)
			AssertErr(t, tx.NewBucket(DataStructureSet, "bucket_dup_2"), ErrBucketAlreadyExist)
			return nil
		})
		require.NoError(t, err)

		assert.True(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))
		assert.True(t, db.bucketMgr.ExistBucket(DataStructureList, bucket))
	})
}

func TestBucketUID502_ConcurrentCreateSameName(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		const bucket = "bucket_concurrent"
		const workers = 16

		var wg sync.WaitGroup
		errs := make([]error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = db.Update(func(tx *Tx) error {
					return tx.NewBucket(DataStructureBTree, bucket)
				})
			}(i)
		}
		wg.Wait()

		var succeeded, duplicated int
		for _, err := range errs {
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrBucketAlreadyExist):
				duplicated++
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		}

		assert.Equal(t, 1, succeeded)
		assert.Equal(t, workers-1, duplicated)
		assert.True(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))
	})
}

func TestBucketUID502_DeleteNonEmptyBucket(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		const bucket = "bucket_non_empty"

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, []byte("key_1"), []byte("value_1"), Persistent, nil, nil)

		txDeleteBucket(t, db, DataStructureBTree, bucket, ErrBucketNotEmpty)
		assert.True(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))
		txGet(t, db, bucket, []byte("key_1"), []byte("value_1"), nil)

		txDel(t, db, bucket, []byte("key_1"), nil)
		txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
		assert.False(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))
	})
}

func TestBucketUID502_SubmitFailureLeavesStateUntouched(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		const bucket = "bucket_submit_failure"

		require.NoError(t, db.bucketMgr.fd.Close())

		err := db.Update(func(tx *Tx) error {
			return tx.NewBucket(DataStructureBTree, bucket)
		})
		require.Error(t, err)
		assert.False(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))

		fd, err := os.OpenFile(
			filepath.Join(db.opt.Dir, BucketStoreFileName),
			os.O_RDWR|os.O_CREATE|os.O_APPEND,
			os.ModePerm,
		)
		require.NoError(t, err)
		db.bucketMgr.fd = fd
		db.bucketMgr.closed = false
	})
}

func TestBucketUID502_RecoveryDedupesDuplicateRecords(t *testing.T) {
	r := require.New(t)
	dir := filepath.Join(t.TempDir(), "nutsdb_test_data")
	const bucket = "bucket_1"

	db, err := Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)
	first, err := db.bucketMgr.GetBucket(DataStructureBTree, bucket)
	r.NoError(err)
	r.NoError(db.Close())

	metaFilePath := filepath.Join(dir, BucketStoreFileName)
	fd, err := os.OpenFile(metaFilePath, os.O_WRONLY|os.O_APPEND, 0o644)
	r.NoError(err)
	duplicate := &core.Bucket{
		Meta: &core.BucketMeta{Op: core.BucketInsertOperation},
		Id:   first.Id + 100,
		Ds:   DataStructureBTree,
		Name: bucket,
	}
	_, err = fd.Write(duplicate.Encode())
	r.NoError(err)
	r.NoError(fd.Close())

	db, err = Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	defer func() {
		if !db.IsClose() {
			r.NoError(db.Close())
		}
	}()

	recovered, err := db.bucketMgr.GetBucket(DataStructureBTree, bucket)
	r.NoError(err)
	assert.Equal(t, duplicate.Id, recovered.Id)

	_, err = db.bucketMgr.GetBucketById(first.Id)
	assert.ErrorIs(t, err, ErrBucketNotExist)

	txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
	assert.False(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))

	r.NoError(db.Close())
	db, err = Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	defer func() { _ = db.Close() }()
	assert.False(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))
}

func TestBucketUID502_RecoveryDoesNotRewriteMetaFile(t *testing.T) {
	r := require.New(t)
	dir := filepath.Join(t.TempDir(), "nutsdb_test_data")
	metaFilePath := filepath.Join(dir, BucketStoreFileName)

	db, err := Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	txCreateBucket(t, db, DataStructureBTree, "bucket_1", nil)
	txCreateBucket(t, db, DataStructureBTree, "bucket_2", nil)
	txDeleteBucket(t, db, DataStructureBTree, "bucket_1", nil)
	r.NoError(db.Close())

	stat, err := os.Stat(metaFilePath)
	r.NoError(err)
	sizeBeforeReopen := stat.Size()

	db, err = Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	r.NoError(db.Close())

	stat, err = os.Stat(metaFilePath)
	r.NoError(err)
	assert.Equal(t, sizeBeforeReopen, stat.Size())

	db, err = Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	defer func() { _ = db.Close() }()
	assert.False(t, db.bucketMgr.ExistBucket(DataStructureBTree, "bucket_1"))
	assert.True(t, db.bucketMgr.ExistBucket(DataStructureBTree, "bucket_2"))
}

func TestBucketUID502_RecoveryAfterRecreate(t *testing.T) {
	r := require.New(t)
	dir := filepath.Join(t.TempDir(), "nutsdb_test_data")
	const bucket = "bucket_recreate"

	db, err := Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)
	txDeleteBucketFunc(t, db, bucket, DataStructureBTree, nil, nil)
	txCreateBucket(t, db, DataStructureBTree, bucket, nil)
	txPut(t, db, bucket, []byte("key_1"), []byte("value_1"), Persistent, nil, nil)
	r.NoError(db.Close())

	db, err = Open(DefaultOptions, WithDir(dir))
	r.NoError(err)
	defer func() { _ = db.Close() }()

	assert.True(t, db.bucketMgr.ExistBucket(DataStructureBTree, bucket))

	bucketCount := 0
	err = db.View(func(tx *Tx) error {
		return tx.IterateBuckets(DataStructureBTree, bucket, func(name string) bool {
			bucketCount++
			return true
		})
	})
	r.NoError(err)
	assert.Equal(t, 1, bucketCount)
	txGet(t, db, bucket, []byte("key_1"), []byte("value_1"), nil)
}
