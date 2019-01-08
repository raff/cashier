/*
This package stores files in BadgerDB, allowing for incremental writes of multiple of BlockSize.
For each file it stores a "metadata" record and a series of "block" records.
Files and data expire after a predefined TTL.
*/
package storage

import (
	"encoding"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"time"
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

// The interface to storage services
type StorageDB interface {
	CreateFile(key, filename, ctype string, size int64, hash []byte) error
	DeleteFile(key string) error
	Close() error
	WriteAt(key string, pos int64, data []byte) (int64, error)
	ReadAt(key string, buf []byte, pos int64) (int64, error)
	Stat(key string) (*FileInfo, error)

	Scan(start string) error
}

// file metadata
type info struct {
	Name        string    `json:"n"` // original file name
	ContentType string    `json:"c"` //
	Hash        string    `json:"h"` // original file hash
	Length      int64     `json:"l"` // original file size
	Created     time.Time `json:"t"` // creation time (time of completion)
	CurPos      int64     `json:"p"` // current offset in file
	CurHash     string    `json:"x"` // current hash
}

func (i *info) Marshal() ([]byte, error) {
	return json.Marshal(i)
}

func (i *info) Unmarshal(data []byte) error {
	return json.Unmarshal(data, i)
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

func infoKey(key string) string {
	return fmt.Sprintf(_INFO, key)
}

func blockKey(key string, block int) string {
	return fmt.Sprintf(_BLOCK, key, block)
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
