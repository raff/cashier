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
	BlockSize          = 16 * 1024
	FileComplete int64 = -1
	InvalidPos   int64 = -2

	_INFO  = "%v:i"
	_BLOCK = "%v:%d"
)

var (
	ErrExists      = fmt.Errorf("File exists")
	ErrNotFound    = fmt.Errorf("File not found")
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
func Open(dataFolder string, readonly bool, ttl time.Duration) (*StorageDB, error) {
	opts := badger.DefaultOptions
	opts.Dir = dataFolder
	opts.ValueDir = dataFolder
	opts.ReadOnly = readonly
	opts.Truncate = true
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &StorageDB{db: db, ttl: ttl}, nil
}

type info struct {
	Name        string    `json:"n"` // original file name
	ContentType string    `json:"c"` //
	Hash        string    `json:"h"` // original file hash
	Length      int64     `json:"l"` // original file size
	Created     time.Time `json:"t"` // creation time (time of completion)
	CurPos      int64     `json:"p"` // current offset in file
	CurHash     string    `json:"x"` // current hash
}

// User file info, returned by Stat
type FileInfo struct {
	Name        string
	ContentType string
	Hash        string
	Length      int64
	Next        int64
	Created     time.Time
	ExpiresAt   time.Time
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
			return ErrExists
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

// Delete file
func (s *StorageDB) DeleteFile(key string) error {
	ikey := fmt.Sprintf(_INFO, key)

	return s.db.Update(func(txn *badger.Txn) error {
		ival, err := txn.Get([]byte(ikey))
		if err == badger.ErrKeyNotFound {
			return nil
		}

		var fileInfo info
		err = ival.Value(func(data []byte) error {
			return json.Unmarshal(data, &fileInfo)
		})
		if err != nil {
			return err
		}

		if err := txn.Delete([]byte(ikey)); err != nil {
			return err
		}

		length := fileInfo.Length
		if fileInfo.CurPos >= 0 { // file not completely written
			length = fileInfo.CurPos
		}

		blocks, rest := length/BlockSize, length%BlockSize
		if rest > 0 {
			blocks += 1
		}

		for i := 0; i < int(blocks); i++ {
			bkey := fmt.Sprintf(_BLOCK, key, i)
			if err := txn.Delete([]byte(bkey)); err != nil {
				log.Println("delete block", i, err)
			}
		}

		return nil
	})
}

// Add data to file
func (s *StorageDB) WriteAt(key string, pos int64, data []byte) (int64, error) {
	if pos < 0 {
		return InvalidPos, ErrInvalidPos
	}

	ikey := fmt.Sprintf(_INFO, key)
	nblocks, rest := len(data)/BlockSize, len(data)%BlockSize
	startBlock, rr := int(pos/BlockSize), int(pos%BlockSize)
	if rr != 0 {
		log.Println(key, "pos", pos, "block", startBlock, "rest", rr)
		return InvalidPos, ErrInvalidPos
	}

	retpos := InvalidPos

	err := s.db.Update(func(txn *badger.Txn) error {
		ival, err := txn.Get([]byte(ikey))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
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
			return ErrExists
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
			if fileInfo.Hash == "" {
				fileInfo.Hash = toHex(hh)
			} else if fileInfo.Hash != toHex(hh) {
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

		fileInfo.Created = time.Now()

		buf, _ := json.Marshal(fileInfo)
		if err := txn.SetWithTTL([]byte(ikey), buf, s.ttl); err != nil {
			return err
		}

		return nil
	})

	return retpos, err
}

func (s *StorageDB) ReadAt(key string, buf []byte, pos int64) (int64, error) {
	ikey := fmt.Sprintf(_INFO, key)
	if pos < 0 {
		return 0, ErrInvalidPos
	}

	block, offs := pos/BlockSize, pos%BlockSize
	nread := int64(0)

	err := s.db.View(func(txn *badger.Txn) error {
		val, err := txn.Get([]byte(ikey))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
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

		for p := 0; lbuf > 0; block += 1 {
			bkey := fmt.Sprintf(_BLOCK, key, block)

			val, err := txn.Get([]byte(bkey))
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}

			val.Value(func(data []byte) error {
				data, offs = data[offs:], 0
				l := len(data)

				if lbuf > l {
					copy(buf[p:], data)
					nread += int64(l)
					lbuf -= l
					p += l
				} else {
					copy(buf[p:], data[:lbuf])
					nread += int64(lbuf)
					p += lbuf
					lbuf = 0
				}

				return nil
			})
		}

		return nil
	})

	return nread, err
}

// Return file info
func (s *StorageDB) Stat(key string) (*FileInfo, error) {
	key = fmt.Sprintf(_INFO, key)

	var stats *FileInfo

	return stats, s.db.View(func(txn *badger.Txn) error {
		val, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
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
			Created:     fileInfo.Created,
			Hash:        fileInfo.Hash,
			Length:      fileInfo.Length,
			Next:        fileInfo.CurPos,
			ExpiresAt:   time.Unix(int64(val.ExpiresAt()), 0),
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
			if item.ExpiresAt() == 0 {
				log.Printf("%v: size=%v", string(item.Key()), item.EstimatedSize())
			} else {
				log.Printf("%v: size=%v exprires=%v deleted=%v",
					string(item.Key()), item.EstimatedSize(),
					time.Unix(int64(item.ExpiresAt()), 0), item.IsDeletedOrExpired())
			}
		}

		return nil
	})
}
