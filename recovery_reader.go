package nutsdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/nutsdb/nutsdb/internal/core"
)

// fileRecovery use bufio.Reader to read entry
type fileRecovery struct {
	fd     *os.File
	reader *bufio.Reader
	size   int64
	closed bool
}

func newFileRecovery(path string, bufSize int) (fr *fileRecovery, err error) {
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	bufSize = calBufferSize(bufSize)
	fileInfo, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	return &fileRecovery{
		fd:     fd,
		reader: bufio.NewReaderSize(fd, bufSize),
		size:   fileInfo.Size(),
		closed: false,
	}, nil
}

// readEntry will read an Entry from disk.
func (fr *fileRecovery) readEntry(off int64) (e *core.Entry, err error) {
	e, _, err = fr.readEntryWithOffset(off)
	return e, err
}

// readEntryWithOffset reads the entry at off and returns the offset immediately
// after it. A non-nil entry may be returned together with ErrCrc so callers can
// inspect the declared record boundaries.
func (fr *fileRecovery) readEntryWithOffset(off int64) (e *core.Entry, nextOff int64, err error) {
	if off < 0 || off > fr.size {
		return nil, off, io.EOF
	}

	headerBuf := make([]byte, core.MaxEntryHeaderSize)
	n, readErr := fr.fd.ReadAt(headerBuf, off)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, off, readErr
	}

	if n < core.MinEntryHeaderSize {
		if n == 0 && errors.Is(readErr, io.EOF) {
			return nil, off, io.EOF
		}
		return nil, off, core.ErrHeaderSizeOutOfBounds
	}

	e = new(core.Entry)
	headerSize, err := e.ParseMeta(headerBuf[:n])
	if err != nil {
		return nil, off, err
	}

	if e.IsZero() {
		return nil, off, nil
	}

	payloadSize := e.Meta.PayloadSize()
	if payloadSize < 0 || headerSize < 0 || off > fr.size-headerSize {
		return nil, off, fmt.Errorf("%w: invalid record length at offset %d", io.ErrUnexpectedEOF, off)
	}

	headerEnd := off + headerSize
	if payloadSize > fr.size-headerEnd {
		return e, off + headerSize + payloadSize, fmt.Errorf(
			"%w: record at offset %d needs %d payload bytes, file size is %d",
			io.ErrUnexpectedEOF, off, payloadSize, fr.size,
		)
	}

	dataBuf := make([]byte, payloadSize)
	if _, err = fr.fd.ReadAt(dataBuf, headerEnd); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return e, off + headerSize + payloadSize, err
	}

	if err = e.ParsePayload(dataBuf); err != nil {
		return e, off + headerSize + payloadSize, err
	}

	headerBuf = headerBuf[:headerSize]
	crc := e.GetCrc(headerBuf)
	if crc != e.Meta.Crc {
		return e, off + headerSize + payloadSize, ErrCrc
	}

	return e, off + headerSize + payloadSize, nil
}

func (fr *fileRecovery) readBucket() (b *core.Bucket, err error) {
	buf := make([]byte, core.BucketMetaSize)
	_, err = io.ReadFull(fr.reader, buf)
	if err != nil {
		return nil, err
	}
	meta := new(core.BucketMeta)
	meta.Decode(buf)
	bucket := new(core.Bucket)
	bucket.Meta = meta
	dataBuf := make([]byte, meta.Size)
	_, err = io.ReadFull(fr.reader, dataBuf)
	if err != nil {
		return nil, err
	}
	err = bucket.Decode(dataBuf)
	if err != nil {
		return nil, err
	}

	if bucket.GetCRC(buf, dataBuf) != bucket.Meta.Crc {
		return nil, core.ErrBucketCrcInvalid
	}

	return bucket, nil
}

// calBufferSize calculates the buffer size of bufio.Reader
// if the size < 4 * KB, use 4 * KB as the size of buffer in bufio.Reader
// if the size > 4 * KB, use the nearly blockSize buffer as the size of buffer in bufio.Reader
func calBufferSize(size int) int {
	blockSize := 4 * KB
	if size < blockSize {
		return blockSize
	}
	hasRest := (size%blockSize == 0)
	if hasRest {
		return (size/blockSize + 1) * blockSize
	}
	return size
}

func (fr *fileRecovery) release() error {
	if fr.closed {
		return nil
	}
	fr.closed = true
	return fr.fd.Close()
}
