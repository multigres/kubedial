package kubedial

import (
	"sync"
	"time"
)

// maxCacheEntries bounds the resolve cache. A process can dial many peers, so
// an unbounded map of addresses would grow without limit; 4096 is far more
// distinct in-cluster addresses than a single client resolves in practice.
const maxCacheEntries = 4096

// cacheEntry is one resolved address. seq orders entries for eviction; it is
// assigned under the lock so "oldest" is the earliest surviving insertion, not
// the earliest with luck in a map walk.
//
// gen is the key's generation. A resolve captures it before looking up and hands
// it to put, which drops a write older than the key's current generation, so a
// lookup that was in flight across an invalidate cannot re-cache a dead
// endpoint. absent marks the tombstone invalidate leaves behind, which keeps the
// generation alive until the lookup it guards has landed.
type cacheEntry struct {
	ep      Endpoint
	ok      bool
	absent  bool
	gen     uint64
	expires time.Time
	seq     uint64
}

// resolveCache caches resolution results keyed by the address string.
//
// Concurrent misses for one key may both look up: resolution is a read of
// cluster state, and the per-key generation makes the later put drop the earlier
// one's answer rather than let it overwrite a newer result. Adding a singleflight
// would serialise dials on a lock for no correctness gain, so it is deliberately
// omitted.
type resolveCache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry
	seq     uint64

	positive time.Duration
	negative time.Duration
	now      func() time.Time
}

// newResolveCache returns a cache whose entries live positive or negative TTLs,
// with time read from now. A test supplies its own now so it never sleeps.
func newResolveCache(now func() time.Time, positive, negative time.Duration) *resolveCache {
	return &resolveCache{
		entries:  make(map[string]cacheEntry),
		positive: positive,
		negative: negative,
		now:      now,
	}
}

// get returns the cached result for addr. found is false for an absent or
// expired entry; an expired entry is dropped as it is found.
func (c *resolveCache) get(addr string) (ep Endpoint, ok, found bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, present := c.entries[addr]
	if !present {
		return Endpoint{}, false, false
	}
	if !c.now().Before(e.expires) {
		delete(c.entries, addr)
		return Endpoint{}, false, false
	}
	if e.absent {
		return Endpoint{}, false, false
	}
	return e.ep, e.ok, true
}

// generation returns the key's current generation; it is 0 for a key the cache
// has never seen. A resolve captures it before it looks up and hands it to put.
func (c *resolveCache) generation(addr string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[addr].gen
}

// put records a result for addr, unless gen is older than the key's current
// generation: an invalidate or another put landed while the caller's lookup was
// in flight, so the caller's answer is stale and must be dropped. A successful
// write advances the generation, so a later write carrying the generation the
// caller read is dropped too. Negative results (ok=false) live the negative TTL,
// which is deliberately longer than the positive one: a name that does not
// resolve is usually an external host that will not become an in-cluster name,
// so it is worth caching for longer than a positive answer, whose pod can move.
//
// It reports whether it wrote. A caller that writes a second key derived from
// the same lookup uses that to keep the two writes together: the second key has
// its own generation, so re-reading it here would let a stale answer re-cache
// under the derived key even after this put was dropped.
func (c *resolveCache) put(addr string, ep Endpoint, ok bool, gen uint64) bool {
	ttl := c.negative
	if ok {
		ttl = c.positive
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if gen < c.entries[addr].gen {
		return false
	}
	if _, present := c.entries[addr]; !present && len(c.entries) >= maxCacheEntries {
		c.evictLocked()
	}
	c.seq++
	c.entries[addr] = cacheEntry{ep: ep, ok: ok, gen: gen + 1, expires: c.now().Add(ttl), seq: c.seq}
	return true
}

// invalidate drops addr's entry so the next dial resolves again. It is called
// when a dial through that address's stream failed. It leaves behind a tombstone
// with a bumped generation, so a lookup that started before the invalidate and
// lands after it cannot put the dead endpoint back; the tombstone is reclaimed
// by the negative TTL, long enough for any in-flight lookup.
func (c *resolveCache) invalidate(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, present := c.entries[addr]; !present && len(c.entries) >= maxCacheEntries {
		c.evictLocked()
	}
	c.seq++
	c.entries[addr] = cacheEntry{absent: true, gen: c.entries[addr].gen + 1, expires: c.now().Add(c.negative), seq: c.seq}
}

// evictLocked makes room for one insertion: first every expired entry, then, if
// the map is still full, the single oldest. It runs only on the overflow path,
// so the O(n) pass is not on the common path.
func (c *resolveCache) evictLocked() {
	now := c.now()
	for key, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, key)
		}
	}
	if len(c.entries) < maxCacheEntries {
		return
	}

	var oldestKey string
	var oldestSeq uint64
	found := false
	for key, e := range c.entries {
		if !found || e.seq < oldestSeq {
			oldestKey, oldestSeq, found = key, e.seq, true
		}
	}
	if found {
		delete(c.entries, oldestKey)
	}
}
