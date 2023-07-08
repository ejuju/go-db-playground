package textdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
)

type DB struct {
	r      *os.File
	w      io.Writer
	wIndex int
	keys   map[string]*ref
}

type ref struct {
	index int
	width int
}

const (
	opSet    = byte('S')
	opDelete = byte('D')
	opPut    = byte('P')

	kPrefix = byte(' ')
	rowEnd  = byte('\n')

	vLenPrefix = byte(' ')
	vPrefix    = byte(' ')
)

func NewDB(fpath string) (*DB, error) {
	db := &DB{keys: make(map[string]*ref)}
	var err error

	// Open read-only file handle and create if needed
	db.r, err = os.OpenFile(fpath, os.O_RDONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// Open write-only file handle in append mode
	db.w, err = os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// Extract existing data from file
	bufr := bufio.NewReader(db.r)
	numRows := 0
	for {
		op, err := bufr.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		numRows++
		db.wIndex++

		if !(op == opSet || op == opDelete || op == opPut) {
			return nil, fmt.Errorf("unknown op: %q (row %d)", op, numRows)
		}

		switch op {
		case opSet, opDelete:
			// Read key-length (with suffix)
			n, kLen, err := db.readLengthWithSuffix(bufr, kPrefix)
			db.wIndex += n
			if err != nil {
				return nil, fmt.Errorf("read key-length: %w (row %d)", err, numRows)
			}

			// Read key (with row-end)
			kWithRowEnd := make([]byte, kLen+1)
			n, err = io.ReadFull(bufr, kWithRowEnd)
			db.wIndex += n
			if err != nil {
				return nil, fmt.Errorf("read key and row-end: %w (row %d)", err, numRows)
			}
			k := string(kWithRowEnd[:kLen])

			// Remove key from refs
			if op == opDelete {
				delete(db.keys, k)
			} else {
				db.keys[k] = nil
			}
		case opPut:
			// Read key-length (with suffix)
			n, kLen, err := db.readLengthWithSuffix(bufr, vLenPrefix)
			db.wIndex += n
			if err != nil {
				return nil, fmt.Errorf("read key-length: %w (row %d)", err, numRows)
			}

			// Read value-length (with suffix)
			n, vLen, err := db.readLengthWithSuffix(bufr, kPrefix)
			db.wIndex += n
			if err != nil {
				return nil, fmt.Errorf("read value-length: %w (row %d)", err, numRows)
			}

			// Read key (with suffix)
			kWithSuffix := make([]byte, kLen+1)
			n, err = io.ReadFull(bufr, kWithSuffix)
			db.wIndex += n
			if err != nil {
				return nil, fmt.Errorf("read key: %w (row %d)", err, numRows)
			}
			k := string(kWithSuffix[:kLen])

			// Read value (with suffix)
			valueStartIndex := db.wIndex
			vWithRowEnd := make([]byte, vLen+1)
			n, err = io.ReadFull(bufr, vWithRowEnd)
			db.wIndex += n
			if err != nil {
				return nil, fmt.Errorf("read value: %w (row %d)", err, numRows)
			}
			v := vWithRowEnd[:vLen]

			// Record key ref
			db.keys[k] = &ref{index: valueStartIndex, width: len(v)}
		}
	}

	return db, nil
}

func (db *DB) readLengthWithSuffix(bufr *bufio.Reader, until byte) (int, int, error) {
	lenWithSuffix, err := bufr.ReadBytes(until)
	if err != nil {
		return len(lenWithSuffix), 0, err
	}
	length, err := strconv.Atoi(string(lenWithSuffix[:len(lenWithSuffix)-1]))
	if err != nil {
		return len(lenWithSuffix), 0, fmt.Errorf("parse integer: %w", err)
	}
	return len(lenWithSuffix), length, nil
}

func (db *DB) ValidateKey(k string) error {
	if len(k) == 0 {
		return errors.New("key is empty")
	}
	if len(k) > math.MaxInt {
		return fmt.Errorf("key is too large: %d (max %d)", len(k), math.MaxInt)
	}
	return nil
}

func (db *DB) Set(k string) error {
	err := db.writeKeyOnlyRow(opSet, k)
	if err != nil {
		return err
	}
	db.keys[k] = nil
	return nil
}

func (db *DB) Delete(k string) error {
	err := db.writeKeyOnlyRow(opDelete, k)
	if err != nil {
		return err
	}
	delete(db.keys, k)
	return nil
}

func (db *DB) writeKeyOnlyRow(op byte, k string) error {
	if err := db.ValidateKey(k); err != nil {
		return err
	}
	var row []byte
	row = append(row, op)
	row = append(row, strconv.Itoa(len(k))...)
	row = append(row, kPrefix)
	row = append(row, k...)
	row = append(row, rowEnd)

	return db.writeAndIncrementOffset(row)
}

func (db *DB) writeAndIncrementOffset(b []byte) error {
	n, err := db.w.Write(b)
	db.wIndex += n
	return err
}

func (db *DB) Put(k string, v []byte) error {
	vStartIndex, err := db.writeKeyValueRow(k, v)
	if err != nil {
		return err
	}
	db.keys[k] = &ref{index: vStartIndex, width: len(v)}
	return nil
}

func (db *DB) writeKeyValueRow(k string, v []byte) (int, error) {
	if err := db.ValidateKey(k); err != nil {
		return 0, err
	}

	var row []byte
	row = append(row, opPut)
	row = append(row, strconv.Itoa(len(k))...)
	row = append(row, vLenPrefix)
	row = append(row, strconv.Itoa(len(v))...)
	row = append(row, kPrefix)
	row = append(row, k...)
	row = append(row, vPrefix)
	vStartIndex := len(row)
	row = append(row, v...)
	row = append(row, rowEnd)

	return vStartIndex, db.writeAndIncrementOffset(row)
}

func (db *DB) Get(k string) ([]byte, error) {
	ref, ok := db.keys[k]
	if !ok {
		return nil, nil
	}
	v := make([]byte, ref.width)
	_, err := db.r.ReadAt(v, int64(ref.index))
	return v, err
}

var ErrKeyNotFound = errors.New("key not found")

func (db *DB) Find(k string) ([]byte, error) {
	v, err := db.Get(k)
	if v == nil && err == nil {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, k)
	}
	return v, err
}

func (db *DB) Exists(k string) bool { _, ok := db.keys[k]; return ok }
