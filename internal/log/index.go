package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// represents the index entries of the records offset.
	// offsets are stored as uint32 so 4 bytes
	offWidth uint64 = 4
	// represents the index entries of the records position.
	// positions are stored as uint64 so 8 bytes
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type Index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*Index, error) {
	idx := &Index{
		file: f,
	}

	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(file.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *Index) Close() error {
	// sync data from memory-mapped file to the persisted file
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// flush persisted file contents to stable storage
	if err := idx.file.Sync(); err != nil {
		return err
	}

	// truncates the persisted file to the amount of data that’s actually in it
	// removing all the empty spaces
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}

	return idx.file.Close()
}

// takes in an offset and returns the associated record’s position in the store
func (idx *Index) Read(in int64) (out uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((idx.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if idx.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(idx.mmap[pos : pos+offWidth])
	pos = enc.Uint64(idx.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// appends the given offset and position to the index
func (idx *Index) Write(off uint32, pos uint64) error {
	if uint64(len(idx.mmap)) < idx.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(idx.mmap[idx.size:idx.size+offWidth], off)
	enc.PutUint64(idx.mmap[idx.size+offWidth:idx.size+entWidth], pos)
	idx.size += uint64(entWidth)

	return nil
}

func (idx *Index) Name() string {
	return idx.file.Name()
}
