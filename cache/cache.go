package cashier

import (
	"sync"
)

type CacheState int

const (
	NEW_ENTRY CacheState = iota
	UPLOADING
	UPLOADED
	PROCESSING
	PROCESSED
	DOWNLOADING
	DONE
)

type CacheEntry struct {
	Key       string
	Operation string
	Input     string
	Output    string
	State     CacheState

	sync.Mutex
	waitInput  *sync.Cond
	waitOutput *sync.Cond
}

func NewCacheEntry(key, operation string) *CacheEntry {
	entry := &CacheEntry{Key: key, Operation: operation}
	entry.waitInput = sync.NewCond(&entry.Mutex)
	entry.waitOutput = sync.NewCond(&entry.Mutex)
	return entry
}

func (c *CacheEntry) WaitInput() {
}

type Cache struct {
	cache map[string]*CacheEntry
	sync.Mutex
}

func NewCache() *Cache {
	return &Cache{cache: make(map[string]*CacheEntry)}
}

func (c *Cache) Get(key string) (ret *CacheEntry) {
	c.Lock()
	ret = c.cache[key]
	c.Unlock()
	return
}

func (c *Cache) Set(key string, value *CacheEntry) (set bool) {
	c.Lock()
	cur := c.cache[key]
	if cur == nil {
		c.cache[key] = value
		set = true
	}
	c.Unlock()
	return
}

var (
	cache = NewCache()
)
