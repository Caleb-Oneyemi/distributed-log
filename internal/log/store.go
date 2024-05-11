package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// the encoding used to persist the record sizes and index entries in the store
var (
	enc = binary.BigEndian
)

// the number of bytes used to store the records length
const (
	lenWidth = 8
)

type Store struct {
	*os.File
	mutex sync.Mutex
	buf   *bufio.Writer
	size  uint64
}

func newStore(f *os.File) (*Store, error) {
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	//needed incase the store is recreated from a file with data
	size := uint64(file.Size())

	return &Store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// writes record bytes to the file buffer.
//
// returns the total number of bytes written and the
// position where the store holds the record in its file
func (s *Store) Append(r []byte) (n uint64, pos uint64, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// where the store holds the record in its file
	pos = s.size

	// write the length of the input string in binary to the file buffer
	// e.g length of 11 is [0 0 0 0 0 0 0 11] and 256 is [0 0 0 0 0 0 1 0]
	// each field is a byte (0 to 255)
	// when reading the record, we use this to know how many characters to read
	if err := binary.Write(s.buf, enc, uint64(len(r))); err != nil {
		return 0, 0, err
	}

	//writes the actual record bytes to the file buffer
	//w ==> len(r)
	w, err := s.buf.Write(r)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// first writes any data in buffer to file.
//
// then finds out how many bytes we have to read to get the whole record,
// and then fetches and returns the record.
func (s *Store) Read(pos uint64) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	// reads len(size) from the file into size starting at offset pos
	// this is the binary representation of the records length
	// e.g length of 11 => [0 0 0 0 0 0 0 11]
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// enc.Uint64(size) converts that representation e.g [0 0 0 0 0 0 1 0]
	// to an unsigned 64 bit integer e.g 256
	b := make([]byte, enc.Uint64(size))
	// reads size number of characters after offset + 8(storage for the length)
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// reads len(r) bytes from the file into r starting at offset off
func (s *Store) ReadAt(r []byte, off int64) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(r, off)
}

// persists buffered data before closing the file
func (s *Store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
