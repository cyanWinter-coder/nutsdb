package nutsdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/nutsdb/nutsdb/internal/core"
)

// verifyHintFile checks every hint offset against the referenced data record.
// A hint is only considered usable when all offsets, lengths and checksums are
// valid. The live index is not modified by this check, allowing the caller to
// fall back to a complete data-file scan without discarding partial index
// state for this file.
func (db *DB) verifyHintFile(fid int64) error {
	hintPath := getHintPath(fid, db.opt.Dir)
	reader := &HintFileReader{}
	if err := reader.Open(hintPath); err != nil {
		return fmt.Errorf("%w: open hint file: %v", ErrHintFileCorrupted, err)
	}
	defer func() {
		_ = reader.Close()
	}()

	dataPath := getDataPath(fid, db.opt.Dir)
	fr, err := newFileRecovery(dataPath, db.opt.BufferSizeOfRecovery)
	if err != nil {
		return fmt.Errorf("open data file for hint verification: %w", err)
	}
	defer func() {
		_ = fr.release()
	}()

	for {
		hint, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: read hint entry: %v", ErrHintFileCorrupted, err)
		}
		if hint == nil {
			continue
		}

		// The hint file name identifies the data file. Older or hand-written
		// hints may leave the per-entry file id zero; applyHintFile normalizes
		// it to fid after validation.
		if hint.DataPos > uint64(fr.size) {
			return fmt.Errorf("%w: data offset %d is outside %s (size %d)",
				ErrHintFileCorrupted, hint.DataPos, dataPath, fr.size)
		}

		entry, nextOff, err := fr.readEntryWithOffset(int64(hint.DataPos))
		if err != nil {
			return fmt.Errorf("%w: data record at offset %d: %v",
				ErrHintFileCorrupted, hint.DataPos, err)
		}
		if entry == nil || entry.Meta == nil {
			return fmt.Errorf("%w: no data record at offset %d", ErrHintFileCorrupted, hint.DataPos)
		}
		if uint64(nextOff) != hint.DataPos+uint64(entry.Size()) {
			return fmt.Errorf("%w: reconstructed end offset %d does not match record length %d at %d",
				ErrHintFileCorrupted, nextOff, entry.Size(), hint.DataPos)
		}
		if nextOff > fr.size {
			return fmt.Errorf("%w: data record at offset %d is truncated", ErrHintFileCorrupted, hint.DataPos)
		}
		if err := hintMatchesEntry(hint, entry); err != nil {
			return err
		}
	}
}

func hintMatchesEntry(hint *HintEntry, entry *core.Entry) error {
	meta := entry.Meta
	mismatch := func(field string, wanted, got any) error {
		return fmt.Errorf("%w: %s mismatch: hint=%v data=%v", ErrHintFileCorrupted, field, wanted, got)
	}

	if hint.BucketId != meta.BucketId {
		return mismatch("bucket id", hint.BucketId, meta.BucketId)
	}
	if hint.KeySize != meta.KeySize {
		return mismatch("key size", hint.KeySize, meta.KeySize)
	}
	if hint.ValueSize != meta.ValueSize {
		return mismatch("value size", hint.ValueSize, meta.ValueSize)
	}
	if hint.Timestamp != meta.Timestamp {
		return mismatch("timestamp", hint.Timestamp, meta.Timestamp)
	}
	if hint.TTL != meta.TTL {
		return mismatch("ttl", hint.TTL, meta.TTL)
	}
	if hint.Flag != meta.Flag {
		return mismatch("flag", hint.Flag, meta.Flag)
	}
	if hint.Status != meta.Status {
		return mismatch("status", hint.Status, meta.Status)
	}
	if hint.Ds != meta.Ds {
		return mismatch("data structure", hint.Ds, meta.Ds)
	}
	if !bytes.Equal(hint.Key, entry.Key) {
		return mismatch("key", string(hint.Key), string(entry.Key))
	}
	if int(hint.KeySize) != len(hint.Key) {
		return mismatch("encoded key size", hint.KeySize, len(hint.Key))
	}

	return nil
}
