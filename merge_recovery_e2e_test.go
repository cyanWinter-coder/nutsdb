package nutsdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// newRecoverableMergeDB opens a v2-enabled database with small segments so
// ordinary test data spans several data files.
func newRecoverableMergeDB(t *testing.T, dir string, hint bool) *DB {
	t.Helper()
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024
	opts.EnableMergeV2 = true
	opts.EnableHintFile = hint
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

// seedMergeData writes enough keys to force multiple data files and returns
// the bucket name and the expected value every key holds.
func seedMergeData(t *testing.T, db *DB, bucket string, numKeys int) []byte {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		return tx.NewBucket(DataStructureBTree, bucket)
	}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	value := bytes.Repeat([]byte("v"), 100)
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, value, Persistent)
		}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	return value
}

func assertAllKeysReadable(t *testing.T, db *DB, bucket string, numKeys int, value []byte) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		for i := 0; i < numKeys; i++ {
			got, err := tx.Get(bucket, []byte(fmt.Sprintf("key-%05d", i)))
			if err != nil {
				return fmt.Errorf("get key-%05d: %w", i, err)
			}
			if !bytes.Equal(got, value) {
				return fmt.Errorf("key-%05d value mismatch", i)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("data verification failed: %v", err)
	}
}

func listFiles(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	files := make(map[string]bool, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			files[e.Name()] = true
		}
	}
	return files
}

// Requirement 3 & 4: a crash during the rewrite or finalize phases must roll
// the merge back. Old data files stay readable, only this merge's outputs are
// removed, and files outside the manifest are untouched.
func TestMergeV2CrashDuringRewriteRollsBack(t *testing.T) {
	for _, hint := range []bool{false, true} {
		t.Run(fmt.Sprintf("hint=%v", hint), func(t *testing.T) {
			dir := t.TempDir()
			db := newRecoverableMergeDB(t, dir, hint)
			const numKeys = 200
			value := seedMergeData(t, db, "bucket", numKeys)

			// A decoy merge file that the manifest must not claim. Recovery
			// has to leave it exactly where it is.
			decoy := "merge_999.dat"
			if err := os.WriteFile(filepath.Join(dir, decoy), []byte("decoy"), 0o644); err != nil {
				t.Fatalf("write decoy: %v", err)
			}

			errCrash := errors.New("simulated crash during rewrite")
			t.Cleanup(func() { mergeV2TestHookPhase = nil })
			mergeV2TestHookPhase = func(phase mergePhase) error {
				if phase == phaseRewriting {
					return errCrash
				}
				return nil
			}

			if err := db.Merge(); !errors.Is(err, errCrash) {
				t.Fatalf("merge: expected crash, got %v", err)
			}
			mergeV2TestHookPhase = nil

			// The in-process abort already removed this merge's outputs and
			// the manifest, but the old files and the decoy remain.
			files := listFiles(t, dir)
			if files[mergeManifestFileName] {
				t.Fatal("manifest should have been removed by abort")
			}
			if !files[decoy] {
				t.Fatal("decoy file outside the manifest must not be removed")
			}
			var userDataFiles int
			for name := range files {
				if filepath.Ext(name) == DataSuffix && !isMergeFileName(name) {
					userDataFiles++
				}
			}
			if userDataFiles < 2 {
				t.Fatalf("expected old data files to survive, found %d", userDataFiles)
			}

			// Reopen: recovery is a no-op on the clean state and all data
			// remains readable.
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			db = newRecoverableMergeDB(t, dir, hint)
			defer func() { _ = db.Close() }()

			assertAllKeysReadable(t, db, "bucket", numKeys, value)
			if !listFiles(t, dir)[decoy] {
				t.Fatal("decoy file must survive reopen")
			}
		})
	}
}

// Requirement 2 & 6: simulate a real crash (no in-process abort) at every
// phase boundary by leaving the manifest on disk, then reopen and verify
// recovery. Rewrite/finalize roll back; committed completes the switch.
func TestMergeV2CrashRecoveryAcrossPhases(t *testing.T) {
	for _, phase := range []mergePhase{phaseRewriting, phaseFinalizing, phaseCommitted} {
		for _, hint := range []bool{false, true} {
			name := fmt.Sprintf("phase=%s/hint=%v", phase, hint)
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				db := newRecoverableMergeDB(t, dir, hint)
				const numKeys = 300
				value := seedMergeData(t, db, "bucket", numKeys)

				// Snapshot the pre-merge files so the rollback cases can
				// assert nothing outside the merge changed.
				before := listFiles(t, dir)
				if err := db.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}

				// Build an on-disk interrupted merge at the requested phase
				// without running abort, exactly as a crashed process would.
				if err := simulateInterruptedMerge(t, dir, hint, phase); err != nil {
					t.Fatalf("simulate interrupted merge: %v", err)
				}

				// Reopen triggers recoverMergeManifest.
				db = newRecoverableMergeDB(t, dir, hint)

				// Repeated recovery: close and reopen a second time. The
				// second recovery must be a no-op.
				if err := db.Close(); err != nil {
					t.Fatalf("close before second reopen: %v", err)
				}
				db = newRecoverableMergeDB(t, dir, hint)
				defer func() { _ = db.Close() }()

				assertAllKeysReadable(t, db, "bucket", numKeys, value)

				after := listFiles(t, dir)
				switch phase {
				case phaseRewriting, phaseFinalizing:
					// Rollback restores the pre-merge file set.
					for name := range before {
						if !after[name] {
							t.Fatalf("rollback lost pre-merge file %s", name)
						}
					}
				case phaseCommitted:
					// Old data files are gone, merge outputs carry the data.
					var mergeOutputs int
					for name := range after {
						if isMergeFileName(name) && filepath.Ext(name) == DataSuffix {
							mergeOutputs++
						}
					}
					if mergeOutputs == 0 {
						t.Fatal("committed recovery should keep merge outputs")
					}
				}
				if after[mergeManifestFileName] {
					t.Fatal("manifest should be removed after recovery")
				}
			})
		}
	}
}

// simulateInterruptedMerge drives a merge job up to the given phase and
// returns with the manifest still on disk, without running abort, so the
// state looks like a process crash.
func simulateInterruptedMerge(t *testing.T, dir string, hint bool, stopAt mergePhase) error {
	t.Helper()
	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024
	opts.EnableMergeV2 = true
	opts.EnableHintFile = hint
	opts.RWMode = FileIO

	db, err := Open(opts)
	if err != nil {
		return err
	}

	job := &mergeV2Job{db: db}
	if err := job.prepare(); err != nil {
		_ = db.Close()
		return err
	}
	if err := job.enterWritingState(); err != nil {
		_ = db.Close()
		return err
	}

	stop := errors.New("stop before abort")
	mergeV2TestHookPhase = func(p mergePhase) error {
		if p == stopAt {
			return stop
		}
		return nil
	}
	defer func() { mergeV2TestHookPhase = nil }()

	runErr := func() error {
		if err := job.persistPhase(phaseRewriting); err != nil {
			return err
		}
		if stopAt == phaseRewriting {
			return stop
		}
		if err := job.rewrite(); err != nil {
			return err
		}
		if err := job.writeHints(); err != nil {
			return err
		}
		if err := job.finalizeOutputs(); err != nil {
			return err
		}
		if err := job.persistPhase(phaseFinalizing); err != nil {
			return err
		}
		if stopAt == phaseFinalizing {
			return stop
		}
		if err := job.commit(); err != nil {
			return err
		}
		return nil
	}()

	mergeV2TestHookPhase = nil

	// Release output handles without removing anything: the process is
	// "dead" and the manifest on disk drives recovery.
	for _, out := range job.outputs {
		if out != nil && !out.finalized {
			_ = out.finalize()
		}
	}
	if cerr := db.Close(); cerr != nil {
		return cerr
	}

	if runErr != nil && !errors.Is(runErr, stop) {
		return runErr
	}
	return nil
}

// Requirement 3: old files referenced by in-memory indexes stay readable for
// the whole merge and are only unlinked after the committed switch. A
// concurrent reader must never hit a missing file.
func TestMergeV2OldFilesStayReadableDuringMerge(t *testing.T) {
	dir := t.TempDir()
	db := newRecoverableMergeDB(t, dir, false)
	const numKeys = 300
	value := seedMergeData(t, db, "bucket", numKeys)

	var readers sync.WaitGroup
	stop := make(chan struct{})
	readErr := make(chan error, 1)

	// Hammer reads continuously while the merge runs. Every record still
	// points at an old data file until commit, so each read must succeed.
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := db.View(func(tx *Tx) error {
					for i := 0; i < numKeys; i += 37 {
						got, err := tx.Get("bucket", []byte(fmt.Sprintf("key-%05d", i)))
						if err != nil {
							return err
						}
						if !bytes.Equal(got, value) {
							return errors.New("value mismatch during merge")
						}
					}
					return nil
				}); err != nil {
					select {
					case readErr <- err:
					default:
					}
					return
				}
			}
		}()
	}

	if err := db.Merge(); err != nil {
		close(stop)
		readers.Wait()
		t.Fatalf("merge: %v", err)
	}

	close(stop)
	readers.Wait()

	select {
	case err := <-readErr:
		t.Fatalf("concurrent read failed while old files were referenced: %v", err)
	default:
	}

	// After the switch, reads must still succeed, now from merge outputs.
	assertAllKeysReadable(t, db, "bucket", numKeys, value)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = newRecoverableMergeDB(t, dir, false)
	defer func() { _ = db.Close() }()
	assertAllKeysReadable(t, db, "bucket", numKeys, value)
}

// Requirement 5: merge concurrent with normal writes. Writes land in the new
// active file and must not block on, or be lost to, the merge. Both the merged
// keys and keys written during the merge survive a reopen.
func TestMergeV2ConcurrentWritesSurviveRecovery(t *testing.T) {
	dir := t.TempDir()
	db := newRecoverableMergeDB(t, dir, true)
	const numKeys = 200
	value := seedMergeData(t, db, "bucket", numKeys)

	mergeDone := make(chan error, 1)
	go func() { mergeDone <- db.Merge() }()

	const concurrentKeys = 50
	var wg sync.WaitGroup
	for i := 0; i < concurrentKeys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := []byte(fmt.Sprintf("concurrent-%03d", i))
			if err := db.Update(func(tx *Tx) error {
				return tx.Put("bucket", []byte(fmt.Sprintf("new-%03d", i)), v, Persistent)
			}); err != nil {
				t.Errorf("concurrent put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if err := <-mergeDone; err != nil {
		t.Fatalf("merge: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = newRecoverableMergeDB(t, dir, true)
	defer func() { _ = db.Close() }()

	assertAllKeysReadable(t, db, "bucket", numKeys, value)
	if err := db.View(func(tx *Tx) error {
		for i := 0; i < concurrentKeys; i++ {
			want := []byte(fmt.Sprintf("concurrent-%03d", i))
			got, err := tx.Get("bucket", []byte(fmt.Sprintf("new-%03d", i)))
			if err != nil {
				return fmt.Errorf("get new-%03d: %w", i, err)
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("new-%03d = %q, want %q", i, got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("verify concurrent writes: %v", err)
	}
}

// Requirement 6: a manifest corrupted after a crash must block Open rather
// than guessing, and leave every file in place for a backup restore.
func TestMergeV2OpenFailsOnCorruptManifestWithoutDeletingFiles(t *testing.T) {
	dir := t.TempDir()
	db := newRecoverableMergeDB(t, dir, false)
	const numKeys = 100
	seedMergeData(t, db, "bucket", numKeys)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	before := listFiles(t, dir)

	// Drop a corrupt manifest into the directory, as a torn write would.
	manifestPath := filepath.Join(dir, mergeManifestFileName)
	if err := os.WriteFile(manifestPath, []byte("{broken"), 0o644); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}

	opts := DefaultOptions
	opts.Dir = dir
	opts.EnableMergeV2 = true
	if _, err := Open(opts); !errors.Is(err, ErrCorruptMergeManifest) {
		t.Fatalf("Open should fail with ErrCorruptMergeManifest, got %v", err)
	}

	after := listFiles(t, dir)
	for name := range before {
		if !after[name] {
			t.Fatalf("corrupt manifest recovery deleted file %s", name)
		}
	}
	if !after[mergeManifestFileName] {
		t.Fatal("corrupt manifest itself must be left in place for inspection")
	}
}

func isMergeFileName(name string) bool {
	_, ok := parseMergeSeq(name)
	return ok
}

// A crash in the preparing phase leaves a manifest with no outputs. Recovery
// only has to drop the manifest; the pending old files are untouched.
func TestMergeV2CrashInPreparingPhase(t *testing.T) {
	dir := t.TempDir()
	db := newRecoverableMergeDB(t, dir, false)
	const numKeys = 100
	value := seedMergeData(t, db, "bucket", numKeys)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	manifest := manifestForOutputs(phasePreparing, nil, nil, false)
	// Enumerate the actual user files as the pending set.
	userIDs, _, err := enumerateDataFileIDs(dir)
	require.NoError(t, err)
	manifest.PendingOldFileIDs = userIDs
	require.NoError(t, writeMergeManifest(dir, manifest))

	before := listFiles(t, dir)
	require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())
	after := listFiles(t, dir)

	for name := range before {
		if name == mergeManifestFileName {
			continue
		}
		if !after[name] {
			t.Fatalf("preparing-phase recovery removed %s", name)
		}
	}
	if after[mergeManifestFileName] {
		t.Fatal("manifest should be removed")
	}

	reopened := newRecoverableMergeDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	assertAllKeysReadable(t, reopened, "bucket", numKeys, value)
}
