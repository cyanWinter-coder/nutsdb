package nutsdb

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/stretchr/testify/require"
)

func startupRebuildOptions(t *testing.T, enableHint bool, mode EntryIdxMode) Options {
	t.Helper()
	opts := DefaultOptions
	opts.Dir = filepath.Join(t.TempDir(), "startup-rebuild")
	opts.SegmentSize = 4 * KB
	opts.EntryIdxMode = mode
	opts.EnableHintFile = enableHint
	opts.SyncEnable = true
	return opts
}

func putStartupKeys(t *testing.T, db *DB, bucket string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
	}
}

func requireStartupKeys(t *testing.T, db *DB, bucket string, n int, missing ...int) {
	t.Helper()
	skip := make(map[int]struct{}, len(missing))
	for _, i := range missing {
		skip[i] = struct{}{}
	}
	for i := 0; i < n; i++ {
		if _, ok := skip[i]; ok {
			txGet(t, db, bucket, testutils.GetTestBytes(i), nil, ErrKeyNotFound)
			continue
		}
		txGet(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), nil)
	}
}

func lastDataRecordForTest(t *testing.T, dir string) (fid int64, off int64, headerSize int64, size int64) {
	t.Helper()
	userIDs, _, err := enumerateDataFileIDs(dir)
	require.NoError(t, err)
	require.NotEmpty(t, userIDs)
	fid = userIDs[len(userIDs)-1]

	fr, err := newFileRecovery(getDataPath(fid, dir), 4096)
	require.NoError(t, err)
	defer func() { require.NoError(t, fr.release()) }()

	var scanOff int64
	for {
		entry, nextOff, readErr := fr.readEntryWithOffset(scanOff)
		if readErr != nil || entry == nil {
			break
		}
		off = scanOff
		headerSize = entry.Meta.Size()
		size = nextOff - scanOff
		scanOff = nextOff
	}
	require.Greater(t, size, headerSize+1)
	return fid, off, headerSize, size
}

func TestStartupRebuildTruncatedRecordIsPrefixRecovered(t *testing.T) {
	for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		t.Run(entryIdxModeName(mode), func(t *testing.T) {
			opts := startupRebuildOptions(t, false, mode)
			const n = 24

			db, err := Open(opts)
			require.NoError(t, err)
			txCreateBucket(t, db, DataStructureBTree, "bucket", nil)
			putStartupKeys(t, db, "bucket", n)
			require.NoError(t, db.Close())

			fid, off, headerSize, recordSize := lastDataRecordForTest(t, opts.Dir)
			dataPath := getDataPath(fid, opts.Dir)

			// Cut one payload byte from the final record. Reopening expands the
			// preallocated segment with zero bytes, so the record fails its
			// checksum and must not be indexed.
			cut := off + headerSize + 1
			require.Less(t, cut, off+recordSize)
			require.NoError(t, os.Truncate(dataPath, cut))

			db, err = Open(opts)
			require.NoError(t, err)

			requireStartupKeys(t, db, "bucket", n, n-1)
			count, err := db.getRecordCount()
			require.NoError(t, err)
			require.Equal(t, int64(n-1), count)
			require.Equal(t, off, db.ActiveFile.writeOff)
			require.Equal(t, off, db.ActiveFile.ActualSize)

			// Appends must start at the recovered prefix. Reopening again
			// proves new writes become visible without reviving the torn tail.
			txPut(t, db, "bucket", []byte("after-prefix"), []byte("ok"), Persistent, nil, nil)
			require.NoError(t, db.Close())

			db, err = Open(opts)
			require.NoError(t, err)
			requireStartupKeys(t, db, "bucket", n, n-1)
			txGet(t, db, "bucket", []byte("after-prefix"), []byte("ok"), nil)
			require.NoError(t, db.Close())
		})
	}
}

func readHintEntriesForTest(t *testing.T, path string) []*HintEntry {
	t.Helper()
	r := &HintFileReader{}
	require.NoError(t, r.Open(path))
	defer func() { require.NoError(t, r.Close()) }()

	var entries []*HintEntry
	for {
		entry, err := r.Read()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		if entry != nil {
			entries = append(entries, entry)
		}
	}
	require.NotEmpty(t, entries)
	return entries
}

func rewriteHintEntriesForTest(t *testing.T, path string, entries []*HintEntry) {
	t.Helper()
	w := &HintFileWriter{}
	require.NoError(t, w.Create(path))
	for _, entry := range entries {
		require.NoError(t, w.Write(entry))
	}
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())
}

func TestStartupRebuildInvalidHintFallsBackToFullScan(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(entries []*HintEntry)
	}{
		{
			name: "offset points one byte away",
			mutate: func(entries []*HintEntry) {
				entries[len(entries)/2].DataPos++
			},
		},
		{
			name: "length mismatch",
			mutate: func(entries []*HintEntry) {
				entries[len(entries)/2].ValueSize++
			},
		},
		{
			name: "offset points to another record",
			mutate: func(entries []*HintEntry) {
				if len(entries) > 1 {
					entries[len(entries)/2].DataPos = entries[0].DataPos
				} else {
					entries[0].DataPos++
				}
			},
		},
	}

	for _, tc := range cases {
		for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
			testName := tc.name + "/" + entryIdxModeName(mode)
			t.Run(testName, func(t *testing.T) {
				opts := startupRebuildOptions(t, true, mode)
				opts.SegmentSize = KB
				const n = 200

				db, err := Open(opts)
				require.NoError(t, err)
				txCreateBucket(t, db, DataStructureBTree, "bucket", nil)
				putStartupKeys(t, db, "bucket", n)
				require.NoError(t, db.Merge())
				require.NoError(t, db.Close())

				hintPaths, err := filepath.Glob(filepath.Join(opts.Dir, "*"+HintSuffix))
				require.NoError(t, err)
				require.NotEmpty(t, hintPaths)

				for _, hintPath := range hintPaths {
					entries := readHintEntriesForTest(t, hintPath)
					tc.mutate(entries)
					rewriteHintEntriesForTest(t, hintPath, entries)
				}

				db, err = Open(opts)
				require.NoError(t, err)
				defer func() { require.NoError(t, db.Close()) }()

				requireStartupKeys(t, db, "bucket", n)
				count, err := db.getRecordCount()
				require.NoError(t, err)
				require.Equal(t, int64(n), count)
				require.Equal(t, n, db.KeyCount)

				tempIndexes, err := filepath.Glob(filepath.Join(opts.Dir, "*.tmp"))
				require.NoError(t, err)
				require.Empty(t, tempIndexes, "fallback must not leave temporary index files")
			})
		}
	}
}

func TestStartupRebuildDiscardsPartialIndex(t *testing.T) {
	db := &DB{}
	db.Index = db.newIndex()
	db.KeyCount = 42
	db.RecordCount = 7

	db.discardRebuildIndex()

	require.NotNil(t, db.Index)
	require.Empty(t, db.Index.BTree.Idx)
	require.Empty(t, db.Index.List.Idx)
	require.Empty(t, db.Index.Set.Idx)
	require.Empty(t, db.Index.SortedSet.Idx)
	require.Zero(t, db.KeyCount)
	require.Zero(t, db.RecordCount)
}

func entryIdxModeName(mode EntryIdxMode) string {
	switch mode {
	case HintKeyValAndRAMIdxMode:
		return "HintKeyValAndRAM"
	case HintKeyAndRAMIdxMode:
		return "HintKeyAndRAM"
	default:
		return "unknown"
	}
}
