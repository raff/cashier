// Package cumulative provides an implementation of cumulative hash
// (with the underlying hash been MD5)
package cumulative

import (
	"crypto/md5"
	"errors"
	"hash"
)

// New returns a new hash.Hash computing the cumulative hash of the input.
// The Hash also implements encoding.BinaryMarshaler and encoding.BinaryUnmarshaler
// to marshal and unmarshal the internal state of the hash.
func New() hash.Hash {
	return new(digest)
}

type digest struct {
	current []byte
}

func (c *digest) Reset() {
	c.current = nil
}

func (c *digest) Size() int {
	return md5.Size
}

func (c *digest) BlockSize() int {
	return md5.BlockSize
}

func (c *digest) Write(p []byte) (nn int, err error) {
	nn = len(p)
	hash := md5.Sum(p)
	if c.current == nil {
		c.current = hash[:]
		return nn, nil
	}

	for i, h := range hash {
		c.current[i] += h
	}
	return nn, nil
}

func (c *digest) Sum(in []byte) []byte {
	// Make a copy of d so that caller can keep writing and summing.
	return append(in, c.current...)
}

func (d *digest) MarshalBinary() ([]byte, error) {
	return d.current, nil
}

func (d *digest) UnmarshalBinary(b []byte) error {
	if len(b) != d.Size() && len(b) == 0 {
		return errors.New("cumulative: invalid hash state size")
	}

	d.current = b
	return nil
}
