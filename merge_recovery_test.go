package nutsdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func newRecoveryDB(dir string) *DB {
	opts := DefaultOptions
	opts.Dir = dir
	return &DB{opt: opts}
}

func touch(t *testing.T, dir, name string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
}

func assertGone(t *testing.T, dir, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	require.Truef(t, os.IsNotExist(err), "%s should be removed, err=%v", name, err)
}

func assertPresent(t *testing.T, dir, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	require.NoErrorf(t, err, "%s should still exist", name)
}

func manifestForOutputs(phase mergePhase, oldIDs []int64, seqs []int, hasHint bool) *mergeManifest {
	m := &mergeManifest{
		Phase:             phase,
		MergeSeqMax:       -1,
		PendingOldFileIDs: oldIDs,
	}
	for _, seq := range seqs {
		out := mergeManifestOutput{
			Seq:    seq,
			Data:   mergeDataFileName(seq),
			Hint:   mergeHintFileName(seq),
			FileID: GetMergeFileID(seq),
		}
		if hasHint {
			out.HasHint = true
		}
		m.Outputs = append(m.Outputs, out)
	}
	if len(seqs) > 0 {
		m.MergeSeqMax = seqs[len(seqs)-1]
	}
	return m
}

// A manifest left in any pre-commit phase rolls the merge back: only the
// outputs it lists are removed, the pending old files and unrelated files are
// preserved, and the manifest is dropped.
func TestRecoverRollsBackInterruptedPhases(t *testing.T) {
	for _, phase := range []mergePhase{phasePreparing, phaseRewriting, phaseFinalizing} {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()

			// Old files that must survive the rollback.
			touch(t, dir, "0.dat")
			touch(t, dir, "0.hint")
			touch(t, dir, "1.dat")

			// Outputs created by the interrupted merge.
			touch(t, dir, "merge_0.dat")
			touch(t, dir, "merge_0.hint")
			touch(t, dir, "merge_1.dat")
			touch(t, dir, "merge_1.hint")

			// A merge file from a different operation, absent from the
			// manifest, must not be touched.
			touch(t, dir, "merge_9.dat")
			touch(t, dir, "merge_9.hint")

			manifest := manifestForOutputs(phase, []int64{0, 1}, []int{0, 1}, true)
			require.NoError(t, writeMergeManifest(dir, manifest))

			require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())

			assertGone(t, dir, "merge_0.dat")
			assertGone(t, dir, "merge_0.hint")
			assertGone(t, dir, "merge_1.dat")
			assertGone(t, dir, "merge_1.hint")
			assertGone(t, dir, mergeManifestFileName)

			assertPresent(t, dir, "0.dat")
			assertPresent(t, dir, "0.hint")
			assertPresent(t, dir, "1.dat")
			assertPresent(t, dir, "merge_9.dat")
			assertPresent(t, dir, "merge_9.hint")
		})
	}
}

// A committed manifest completes the switch: the pending old data and hint
// files are removed while the merge outputs stay, and unrelated files remain.
func TestRecoverFinishesCommittedMerge(t *testing.T) {
	dir := t.TempDir()

	touch(t, dir, "0.dat")
	touch(t, dir, "0.hint")
	touch(t, dir, "1.dat")
	touch(t, dir, "merge_0.dat")
	touch(t, dir, "merge_0.hint")
	// Unrelated merge file, not part of this manifest.
	touch(t, dir, "merge_7.dat")

	manifest := manifestForOutputs(phaseCommitted, []int64{0, 1}, []int{0}, true)
	require.NoError(t, writeMergeManifest(dir, manifest))

	require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())

	assertGone(t, dir, "0.dat")
	assertGone(t, dir, "0.hint")
	assertGone(t, dir, "1.dat")
	assertGone(t, dir, mergeManifestFileName)

	assertPresent(t, dir, "merge_0.dat")
	assertPresent(t, dir, "merge_0.hint")
	assertPresent(t, dir, "merge_7.dat")
}

// Recovery must be safe to run repeatedly: once the old files are gone, a
// second recovery is a no-op instead of failing on the missing files.
func TestRecoverCommittedMergeIsIdempotent(t *testing.T) {
	dir := t.TempDir()

	touch(t, dir, "0.dat")
	touch(t, dir, "merge_0.dat")
	manifest := manifestForOutputs(phaseCommitted, []int64{0}, []int{0}, false)
	require.NoError(t, writeMergeManifest(dir, manifest))

	db := newRecoveryDB(dir)
	require.NoError(t, db.recoverMergeManifest())
	assertGone(t, dir, "0.dat")
	assertGone(t, dir, mergeManifestFileName)

	// The second run finds no manifest at all.
	require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())

	// Simulate a crash between the cleaned-phase write and the manifest
	// removal: recovery must only drop the manifest.
	touch(t, dir, "1.dat")
	cleaned := manifestForOutputs(phaseCleaned, []int64{1}, []int{0}, false)
	require.NoError(t, writeMergeManifest(dir, cleaned))
	require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())
	assertGone(t, dir, mergeManifestFileName)
	assertPresent(t, dir, "merge_0.dat")
}

// Rollback recovery repeated after a partial cleanup must converge: a second
// run with the same manifest still removes every listed output.
func TestRecoverRollbackIsIdempotent(t *testing.T) {
	dir := t.TempDir()

	touch(t, dir, "0.dat")
	touch(t, dir, "merge_0.dat")
	touch(t, dir, "merge_1.dat")
	manifest := manifestForOutputs(phaseRewriting, []int64{0}, []int{0, 1}, false)
	require.NoError(t, writeMergeManifest(dir, manifest))

	db := newRecoveryDB(dir)
	require.NoError(t, db.recoverMergeManifest())

	// Recreate the manifest as if a fresh interrupted merge used the same
	// output seqs, then recover again.
	touch(t, dir, "merge_0.dat")
	require.NoError(t, writeMergeManifest(dir, manifest))
	require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())

	assertGone(t, dir, "merge_0.dat")
	assertGone(t, dir, "merge_1.dat")
	assertPresent(t, dir, "0.dat")
}

// A torn or tampered manifest must fail loudly without deleting anything.
func TestRecoverRejectsCorruptManifest(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "0.dat")
	touch(t, dir, "merge_0.dat")

	t.Run("invalid JSON", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, mergeManifestFileName), []byte("{not json"), 0o644))
		err := newRecoveryDB(dir).recoverMergeManifest()
		require.ErrorIs(t, err, ErrCorruptMergeManifest)
		assertPresent(t, dir, "0.dat")
		assertPresent(t, dir, "merge_0.dat")
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		manifest := manifestForOutputs(phaseRewriting, []int64{0}, []int{0}, false)
		require.NoError(t, writeMergeManifest(dir, manifest))

		// Tamper with the persisted checksum; the content fields no longer
		// match it, so recovery must reject the manifest.
		manifest.Checksum++
		data, err := json.Marshal(manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, mergeManifestFileName), data, 0o644))

		err = newRecoveryDB(dir).recoverMergeManifest()
		require.ErrorIs(t, err, ErrCorruptMergeManifest)
		assertPresent(t, dir, "0.dat")
		assertPresent(t, dir, "merge_0.dat")
	})

	t.Run("unknown phase", func(t *testing.T) {
		manifest := manifestForOutputs(mergePhase("nonsense"), []int64{0}, nil, false)
		require.NoError(t, writeMergeManifest(dir, manifest))
		err := newRecoveryDB(dir).recoverMergeManifest()
		require.ErrorIs(t, err, ErrCorruptMergeManifest)
		assertPresent(t, dir, "0.dat")
	})
}

// A committed recovery must refuse to delete old files when an output is
// missing: completing the switch would lose data, so it errors and leaves the
// directory (and backup) untouched.
func TestRecoverCommittedFailsWhenOutputMissing(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "0.dat")
	// merge_0.dat intentionally absent.
	manifest := manifestForOutputs(phaseCommitted, []int64{0}, []int{0}, false)
	require.NoError(t, writeMergeManifest(dir, manifest))

	err := newRecoveryDB(dir).recoverMergeManifest()
	require.Error(t, err)
	require.Contains(t, err.Error(), "committed merge output missing")
	assertPresent(t, dir, "0.dat")
	assertPresent(t, dir, mergeManifestFileName)
}

// Manifests written by older builds only carry the legacy status field and no
// checksum. They must still load and recover the same way.
func TestRecoverLegacyManifest(t *testing.T) {
	writeLegacy := func(t *testing.T, dir, status string, oldIDs []int64) {
		t.Helper()
		content := `{"status":"` + status + `","mergeSeqMax":-1,"pendingOldFileIDs":[`
		for i, id := range oldIDs {
			if i > 0 {
				content += ","
			}
			content += strconv.FormatInt(id, 10)
		}
		content += `]}`
		require.NoError(t, os.WriteFile(filepath.Join(dir, mergeManifestFileName), []byte(content), 0o644))
	}

	t.Run("legacy writing rolls back", func(t *testing.T) {
		dir := t.TempDir()
		touch(t, dir, "0.dat")
		touch(t, dir, "merge_0.dat")
		writeLegacy(t, dir, manifestStatusWriting, []int64{0})

		require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())
		assertPresent(t, dir, "0.dat")
		assertGone(t, dir, mergeManifestFileName)
	})

	t.Run("legacy committed completes", func(t *testing.T) {
		dir := t.TempDir()
		touch(t, dir, "0.dat")
		writeLegacy(t, dir, manifestStatusCommitted, []int64{0})

		require.NoError(t, newRecoveryDB(dir).recoverMergeManifest())
		assertGone(t, dir, "0.dat")
		assertGone(t, dir, mergeManifestFileName)
	})
}
