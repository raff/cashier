/*
This package stores files in BadgerDB, allowing for incremental writes of multiple of BlockSize.
For each file it stores a "metadata" record and a series of "block" records.
Files and data expire after a predefined TTL.
*/
package storage

import (
	"crypto/md5"
	"encoding"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"time"

	"github.com/dgraph-io/badger"
)

const (
	BlockSize    = 16 * 1024
	FileComplete = -1

	_INFO  = "%v:i"
	_BLOCK = "%v:%d"
)

var (
	ErrFileExists  = fmt.Errorf("File exists")
	ErrInvalidSize = fmt.Errorf("Invalid Size")
	ErrInvalidPos  = fmt.Errorf("Invalid Position")
	ErrInvalidHash = fmt.Errorf("Invalid Hash")
	ErrIncomplete  = fmt.Errorf("File incomplete")
)

// An instance of the Storage service
type StorageDB struct {
	db  *badger.DB
	ttl time.Duration
}

// Open data folder and return instance of storage service
func Open(dataFolder string, ttl time.Duration) (*StorageDB, error) {
	opts := badger.DefaultOptions
	opts.Dir = dataFolder
	opts.ValueDir = dataFolder
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &StorageDB{db: db, ttl: ttl}, nil
}

type info struct {
	Name        string `json:"n"` // original file name
	ContentType string `json:"c"` //
	Hash        string `json:"h"` // original file hash
	Length      int64  `json:"l"` // original file size
	CurPos      int64  `json:"p"` // current offset in file
	CurHash     string `json:"x"` // current hash
}

// User file info, returned by Stat
type FileInfo struct {
	Name        string
	ContentType string
	Hash        string
	Length      int64
	Next        int64
}

// Close storage service
func (s *StorageDB) Close() error {
	return s.db.Close()
}

// Run garbage collector
func (s *StorageDB) GC() error {
	return s.db.RunValueLogGC(0.5)
}

func toHex(b []byte) string {
	return fmt.Sprintf("%x", b)
}

func fromHex(s string) []byte {
	var b []byte
	fmt.Sscanf(s, "%x", &b)
	return b
}

func marshalHash(h hash.Hash) (string, error) {
	marshaler, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		log.Fatal("hash does not implement encoding.BinaryMarshaler")
	}
	state, err := marshaler.MarshalBinary()
	if err != nil {
		return "", err
	}

	return toHex(state), nil
}

func unmarshalHash(h hash.Hash, state string) error {
	if state == "" {
		return nil
	}

	unmarshaler, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		log.Fatal("hash does not implement encoding.BinaryUnmarshaler")
	}
	return unmarshaler.UnmarshalBinary(fromHex(state))
}

// Create new file, by adding the file info
func (s *StorageDB) CreateFile(key, filename, ctype string, size int64, hash []byte) error {
	key = fmt.Sprintf(_INFO, key)
	data, _ := json.Marshal(info{Name: filename, ContentType: ctype, Length: size, Hash: toHex(hash[:])})
	return s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			return ErrFileExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// write file Info
		if err = txn.SetWithTTL([]byte(key), data, s.ttl); err != nil {
			return err
		}

		return nil
	})
}

// Add data to file
func (s *StorageDB) WriteData(key string, pos int64, data []byte) (int64, error) {
	retpos := int64(-2)
	if pos < 0 {
		return retpos, ErrInvalidPos
	}

	ikey := fmt.Sprintf(_INFO, key)
	nblocks, rest := len(data)/BlockSize, len(data)%BlockSize
	startBlock, rr := int(pos/BlockSize), int(pos%BlockSize)
	if rr != 0 {
		log.Println(key, "pos", pos, "block", startBlock, "rest", rr)
		return retpos, ErrInvalidPos
	}

	return retpos, s.db.Update(func(txn *badger.Txn) error {
		ival, err := txn.Get([]byte(ikey))
		if err == badger.ErrKeyNotFound {
			return err
		}

		var fileInfo info
		err = ival.Value(func(data []byte) error {
			return json.Unmarshal(data, &fileInfo)
		})
		if err != nil {
			return err
		}

		//log.Println(fileInfo, "start", startBlock, "blocks", nblocks, "rest", rest, "pos", pos)

		if fileInfo.CurPos < 0 { // file complete
			return ErrFileExists
		}

		if pos != fileInfo.CurPos { // wrong start
			log.Println(fileInfo.Name, "block", startBlock, "pos", pos, "cur", fileInfo.CurPos)
			return ErrInvalidPos
		}

		if pos+int64(len(data)) > fileInfo.Length { // out of boundary
			log.Println(fileInfo.Name, "block", startBlock, "pos", pos, "data", len(data), "file", fileInfo.Length)
			return ErrInvalidSize
		}

		fblocks := int(fileInfo.Length / BlockSize)

		if startBlock+nblocks < fblocks && rest != 0 {
			log.Println(fileInfo.Name, "block", startBlock, "pos", pos, "n", nblocks, "file", fblocks, "rest", rest)
			return ErrInvalidSize
		}

		if pos+int64(len(data)) == fileInfo.Length && rest > 0 {
			nblocks += 1
		}

		block := startBlock
		offs := int64(0)
		ldata := len(data)

		curHash := md5.New()
		if err := unmarshalHash(curHash, fileInfo.CurHash); err != nil {
			return err
		}

		for ldata > 0 {
			bkey := fmt.Sprintf(_BLOCK, key, block)
			buf := data[offs:]
			if len(buf) > BlockSize {
				buf = buf[:BlockSize]
			}

			err = txn.SetWithTTL([]byte(bkey), buf, s.ttl)
			if err != nil {
				return err
			}

			curHash.Write(buf)

			block += 1
			offs += int64(len(buf))
			ldata -= len(buf)
		}

		hh := curHash.Sum(nil)
		if fileInfo.CurPos+offs == fileInfo.Length { // we are done
			if fileInfo.Hash != toHex(hh) {
				// delete file ?
				return ErrInvalidHash
			}

			retpos = FileComplete
			fileInfo.CurPos = FileComplete
			fileInfo.CurHash = ""
		} else {
			fileInfo.CurHash, err = marshalHash(curHash)
			if err != nil {
				return err
			}

			fileInfo.CurPos += offs
			retpos = fileInfo.CurPos
		}

		buf, _ := json.Marshal(fileInfo)
		if err := txn.SetWithTTL([]byte(ikey), buf, s.ttl); err != nil {
			return err
		}

		return nil
	})
}

func (s *StorageDB) ReadAt(buf []byte, pos int64) (int64, error) {
	key = fmt.Sprintf(_INFO, key)
	nread := int64(0)
	if pos < 0 {
		return nread, ErrInvalidPos
	}

	block, rest := pos/BlockSize, pos%BlockSize

	return nread, s.db.View(func(txn *badger.Txn) error {
		val, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return err
		}

		var fileInfo info

		err = val.Value(func(data []byte) error {
			return json.Unmarshal(data, &fileInfo)
		})
		if err != nil {
			return err
		}

		if fileInfo.CurPos != FileComplete {
			return ErrIncomplete
		}

		if pos > fileInfo.Length {
			return ErrInvalidPos
		}

		lbuf := len(buf)
		if int(fileInfo.Length-pos) < lbuf {
			lbuf = int(fileInfo.Length - pos)
		}

		return nil
	})
}

// Return file info
func (s *StorageDB) Stat(key string) (*FileInfo, error) {
	key = fmt.Sprintf(_INFO, key)

	var stats *FileInfo

	return stats, s.db.View(func(txn *badger.Txn) error {
		val, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return err
		}

		var fileInfo info

		err = val.Value(func(data []byte) error {
			return json.Unmarshal(data, &fileInfo)
		})
		if err != nil {
			return err
		}

		stats = &FileInfo{
			Name:        fileInfo.Name,
			ContentType: fileInfo.ContentType,
			Hash:        fileInfo.Hash,
			Length:      fileInfo.Length,
			Next:        fileInfo.CurPos,
		}

		return nil
	})
}

// Scan database, for debugging purposes
func (s *StorageDB) Scan(start string) error {
	key := []byte(start)

	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(key); it.Valid(); it.Next() {
			item := it.Item()
			log.Println(string(item.Key()), item.EstimatedSize(),
				time.Unix(int64(item.ExpiresAt()), 0), item.IsDeletedOrExpired())
		}

		return nil
	})
}