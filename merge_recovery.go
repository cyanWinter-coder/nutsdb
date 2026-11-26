package nutsdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// recoverMergeManifest finishes or rolls back a merge that was interrupted by
// a process crash or a machine restart. It runs once during Open, before any
// index is built, while the directory lock is held.
//
// The behavior is driven entirely by the persisted phase:
//
//   - preparing / rewriting / finalizing: the switch never happened, so the
//     pending old files are still the source of truth. The merge outputs are
//     intermediate files and are removed, then the manifest is dropped.
//   - committed: the switch is durable. The merge outputs are complete, so
//     the pending old files (data + hint) are removed.
//   - cleaned: the old files were already removed in a previous recovery;
//     only the manifest remains. This makes repeated recovery idempotent.
//
// Only files enumerated by the manifest are ever removed. Other files in the
// directory, including merge files that belong to a different operation, are
// left exactly as found.
func (db *DB) recoverMergeManifest() error {
	manifest, err := loadMergeManifest(db.opt.Dir)
	if err != nil {
		// A corrupt manifest is surfaced verbatim and nothing is deleted, so
		// an operator can restore the directory from backup and retry.
		return err
	}
	if manifest == nil {
		return nil
	}

	switch manifest.Phase {
	case phasePreparing, phaseRewriting, phaseFinalizing:
		if err := rollbackInterruptedMerge(db.opt.Dir, manifest); err != nil {
			return err
		}
	case phaseCommitted:
		if err := finishCommittedMerge(db.opt.Dir, manifest); err != nil {
			return err
		}
	case phaseCleaned:
		// A previous recovery already deleted the old files; just drop the
		// manifest. Reaching this phase twice must be a no-op.
		if err := removeMergeManifest(db.opt.Dir); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unknown merge phase %q", ErrCorruptMergeManifest, manifest.Phase)
	}

	return nil
}

// rollbackInterruptedMerge deletes the merge outputs recorded in the
// manifest and the manifest itself. The pending old files are never touched,
// which returns the directory to its pre-merge state.
func rollbackInterruptedMerge(dir string, manifest *mergeManifest) error {
	if err := removeManifestOutputs(dir, manifest); err != nil {
		return fmt.Errorf("rollback merge outputs: %w", err)
	}
	if err := removeMergeManifest(dir); err != nil {
		return err
	}
	return nil
}

// finishCommittedMerge removes the old data and hint files that the merge
// replaced, then records the cleaned phase and drops the manifest. The merge
// outputs are verified to exist before any old file is removed: if an output
// is missing, the committed switch cannot be completed safely and the error
// is returned without deleting anything.
func finishCommittedMerge(dir string, manifest *mergeManifest) error {
	for _, out := range manifest.Outputs {
		if err := requireFile(filepath.Join(dir, filepath.Base(out.Data))); err != nil {
			return fmt.Errorf("committed merge output missing: %w", err)
		}
		if out.HasHint {
			if err := requireFile(filepath.Join(dir, filepath.Base(out.Hint))); err != nil {
				return fmt.Errorf("committed merge hint missing: %w", err)
			}
		}
	}

	for _, fid := range manifest.PendingOldFileIDs {
		if err := removeIfExists(getDataPath(fid, dir)); err != nil {
			return fmt.Errorf("remove old data file %d: %w", fid, err)
		}
		if err := removeIfExists(getHintPath(fid, dir)); err != nil {
			return fmt.Errorf("remove old hint file %d: %w", fid, err)
		}
	}

	// Persist the cleaned phase before dropping the manifest. If this process
	// dies here, the next recovery sees phaseCleaned and only removes the
	// manifest instead of re-deleting (harmlessly) or failing on missing
	// files.
	manifest.Phase = phaseCleaned
	if err := manifest.persist(dir); err != nil {
		return fmt.Errorf("record cleaned phase: %w", err)
	}
	return removeMergeManifest(dir)
}

// removeManifestOutputs deletes only the data/hint files listed in the
// manifest. Files outside that list, including merge files from another
// operation, are not touched.
func removeManifestOutputs(dir string, manifest *mergeManifest) error {
	for _, out := range manifest.Outputs {
		if err := removeIfExists(filepath.Join(dir, filepath.Base(out.Data))); err != nil {
			return fmt.Errorf("remove merge data %s: %w", out.Data, err)
		}
		if out.HasHint {
			if err := removeIfExists(filepath.Join(dir, filepath.Base(out.Hint))); err != nil {
				return fmt.Errorf("remove merge hint %s: %w", out.Hint, err)
			}
		}
	}
	return nil
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func requireFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("%s is a directory", path)
	}
	return nil
}
