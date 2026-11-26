package nutsdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"path/filepath"
	"syscall"
)

// mergeManifestStatus is the legacy two-state field kept for reading
// manifests written by older builds. New manifests use Phase.
type mergeManifestStatus string

// newManifestHasher returns the FNV-1a 32-bit hasher used for the manifest
// checksum. FNV is deterministic, allocation-light and needs no key material.
func newManifestHasher() hash.Hash32 {
	return fnv.New32a()
}

// mergePhase is the position of a merge inside its staged, recoverable
// pipeline. The value is persisted in the merge manifest so that, after a
// restart, recovery knows whether the merge outputs can be trusted (and the
// old files removed) or the outputs are intermediate garbage (and the old
// files are the source of truth).
type mergePhase string

const (
	mergeManifestFileName       = "merge_manifest.json"
	mergeManifestTempFileSuffix = ".tmp"

	// Legacy statuses, kept so manifests written by older builds still load.
	manifestStatusWriting   = "writing"
	manifestStatusCommitted = "committed"

	// phasePreparing: the manifest has been written but no output exists yet.
	phasePreparing mergePhase = "preparing"
	// phaseRewriting: at least one merge output is being filled. Outputs are
	// intermediate and must not survive a crash.
	phaseRewriting mergePhase = "rewriting"
	// phaseFinalizing: every output is synced and closed. Hint files are on
	// disk, but the indexes have not switched yet.
	phaseFinalizing mergePhase = "finalizing"
	// phaseCommitted: the manifest switch is durable. The outputs replace the
	// pending old files, which recovery now removes.
	phaseCommitted mergePhase = "committed"
	// phaseCleaned: the old files are gone; only the manifest still needs to
	// be removed.
	phaseCleaned mergePhase = "cleaned"
)

// mergeManifest is the durable record of one merge operation.
//
// It enumerates both sides of the switch:
//   - PendingOldFileIDs: files that are read while merging and stay readable
//     until the switch completes; nothing here is deleted before the
//     committed phase.
//   - Outputs: merge data/hint files produced by this merge. Recovery only
//     ever removes files listed here, so files that do not belong to the
//     merge are never touched.
type mergeManifest struct {
	// Phase is the current pipeline stage. New manifests use Phase; manifests
	// written by older builds only carry Status, which is migrated on load.
	Phase mergePhase `json:"phase"`

	// MergeSeqMax is the highest merge sequence produced by this merge, -1
	// when the merge produced no output.
	MergeSeqMax int `json:"mergeSeqMax"`

	// PendingOldFileIDs are the data files that participate in the merge.
	PendingOldFileIDs []int64 `json:"pendingOldFileIDs"`

	// Outputs lists every merge file this merge created, in sequence order.
	Outputs []mergeManifestOutput `json:"outputs"`

	// Checksum covers the fields above. A mismatched checksum means the
	// manifest was torn or tampered with; recovery refuses to guess and
	// leaves the directory untouched for a backup restore.
	Checksum uint32 `json:"checksum"`

	// Status is retained for backward compatibility with older manifests.
	// New writes leave it empty and rely on Phase.
	Status mergeManifestStatus `json:"status,omitempty"`
}

// mergeManifestOutput identifies one merge output pair on disk.
type mergeManifestOutput struct {
	Seq     int    `json:"seq"`
	Data    string `json:"data"`
	Hint    string `json:"hint"`
	FileID  int64  `json:"fileID"`
	HasHint bool   `json:"hasHint"`
}

// ErrCorruptMergeManifest is returned when a manifest exists but cannot be
// decoded or fails its checksum. No file is removed in that case.
var ErrCorruptMergeManifest = errors.New("corrupt merge manifest")

// manifestChecksum computes an FNV-1a checksum over the content fields. It
// deliberately excludes Checksum itself.
func manifestChecksum(m *mergeManifest) uint32 {
	h := newManifestHasher()
	fmt.Fprintf(h, "phase=%s|seqMax=%d|old=%v|outputs=[", m.Phase, m.MergeSeqMax, m.PendingOldFileIDs)
	for _, o := range m.Outputs {
		fmt.Fprintf(h, "{seq=%d,data=%s,hint=%s,fid=%d,hasHint=%t}", o.Seq, o.Data, o.Hint, o.FileID, o.HasHint)
	}
	_, _ = h.Write([]byte("]"))
	return h.Sum32()
}

func loadMergeManifest(dir string) (*mergeManifest, error) {
	path := filepath.Join(dir, mergeManifestFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read merge manifest: %w", err)
	}

	var manifest mergeManifest
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrCorruptMergeManifest, err)
	}

	manifest.migrateLegacyStatus()

	// Manifests written before checksumming carry Checksum == 0. Only enforce
	// the checksum when one was recorded, so old manifests still recover.
	if manifest.Checksum != 0 && manifestChecksum(&manifest) != manifest.Checksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptMergeManifest)
	}

	if err := manifest.validate(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// migrateLegacyStatus maps the old two-state Status field onto Phase.
func (m *mergeManifest) migrateLegacyStatus() {
	if m.Phase != "" {
		return
	}
	switch m.Status {
	case manifestStatusCommitted:
		m.Phase = phaseCommitted
	case manifestStatusWriting:
		m.Phase = phaseRewriting
	default:
		// Unknown legacy status: leave Phase empty so validate() rejects it.
	}
}

func (m *mergeManifest) validate() error {
	switch m.Phase {
	case phasePreparing, phaseRewriting, phaseFinalizing, phaseCommitted, phaseCleaned:
	default:
		return fmt.Errorf("%w: unknown merge phase %q", ErrCorruptMergeManifest, m.Phase)
	}
	for _, o := range m.Outputs {
		if o.FileID != GetMergeFileID(o.Seq) {
			return fmt.Errorf("%w: output seq %d does not match file id %d", ErrCorruptMergeManifest, o.Seq, o.FileID)
		}
	}
	return nil
}

// persist stamps the checksum and atomically replaces the manifest via a
// temp file + fsync + rename, so a torn write or a power loss can never leave
// a half-written manifest: either the previous version or the complete new one
// is on disk. The directory is synced too so the rename itself is durable.
func (m *mergeManifest) persist(dir string) error {
	m.Status = ""
	m.Checksum = manifestChecksum(m)
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode merge manifest: %w", err)
	}
	manifestPath := filepath.Join(dir, mergeManifestFileName)
	tmpPath := manifestPath + mergeManifestTempFileSuffix

	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open manifest tmp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write manifest tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync manifest tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close manifest tmp: %w", err)
	}
	if err := os.Rename(tmpPath, manifestPath); err != nil {
		return fmt.Errorf("rename manifest tmp: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync manifest directory: %w", err)
	}
	return nil
}

// syncDir flushes directory entry changes (creation, rename, unlink) to disk.
// Filesystems that do not support fsync on a directory return EINVAL; that
// cannot be acted on, so it is ignored rather than failing the merge.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}

// writeMergeManifest keeps the package-level helper used by existing callers
// and tests; it routes through persist so checksums stay consistent.
func writeMergeManifest(dir string, manifest *mergeManifest) error {
	return manifest.persist(dir)
}

func removeMergeManifest(dir string) error {
	path := filepath.Join(dir, mergeManifestFileName)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove merge manifest: %w", err)
	}
	tmpPath := path + mergeManifestTempFileSuffix
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove merge manifest tmp: %w", err)
	}
	return nil
}
