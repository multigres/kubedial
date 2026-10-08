package kubedial

import (
	"fmt"
	"testing"
	"time"
)

func TestCachePositiveTTL(t *testing.T) {
	now := time.Unix(0, 0)
	c := newResolveCache(func() time.Time { return now }, 10*time.Second, 30*time.Second)
	want := Endpoint{Namespace: "ns", Pod: "p", PodIP: "10.1.0.5", Port: 5432}
	c.put("svc.ns:5432", want, true, 0)

	got, ok, found := c.get("svc.ns:5432")
	if !found || !ok {
		t.Fatalf("get = found=%v ok=%v, want found=true ok=true", found, ok)
	}
	if got != want {
		t.Fatalf("endpoint = %+v, want %+v", got, want)
	}

	now = now.Add(9 * time.Second)
	if _, _, found := c.get("svc.ns:5432"); !found {
		t.Error("positive entry expired before its TTL")
	}

	now = now.Add(2 * time.Second)
	if _, _, found := c.get("svc.ns:5432"); found {
		t.Error("positive entry survived past its TTL")
	}
}

func TestCacheNegativeTTL(t *testing.T) {
	now := time.Unix(0, 0)
	c := newResolveCache(func() time.Time { return now }, 10*time.Second, 30*time.Second)
	c.put("other.ns:80", Endpoint{}, false, 0)

	_, ok, found := c.get("other.ns:80")
	if !found || ok {
		t.Fatalf("get = found=%v ok=%v, want found=true ok=false", found, ok)
	}

	now = now.Add(29 * time.Second)
	if _, _, found := c.get("other.ns:80"); !found {
		t.Error("negative entry expired before its TTL")
	}

	now = now.Add(2 * time.Second)
	if _, _, found := c.get("other.ns:80"); found {
		t.Error("negative entry survived past its TTL")
	}
}

func TestCacheBounded(t *testing.T) {
	now := time.Unix(0, 0)
	c := newResolveCache(func() time.Time { return now }, time.Hour, time.Hour)

	// One more than the bound, all live, so eviction has to fall back from
	// dropping expired entries to dropping the oldest.
	for i := range maxCacheEntries + 1 {
		c.put(fmt.Sprintf("addr-%d:80", i), Endpoint{Pod: "p"}, true, 0)
	}

	c.mu.Lock()
	size := len(c.entries)
	_, oldest := c.entries["addr-0:80"]
	_, newest := c.entries[fmt.Sprintf("addr-%d:80", maxCacheEntries)]
	c.mu.Unlock()

	if size != maxCacheEntries {
		t.Errorf("cache size = %d, want %d", size, maxCacheEntries)
	}
	if oldest {
		t.Error("oldest entry was not evicted on overflow")
	}
	if !newest {
		t.Error("newest entry is missing after overflow")
	}
}

func TestCacheEvictsExpiredFirst(t *testing.T) {
	now := time.Unix(0, 0)
	c := newResolveCache(func() time.Time { return now }, time.Hour, time.Hour)

	const expired = 10
	for i := range expired {
		c.put(fmt.Sprintf("exp-%d:80", i), Endpoint{Pod: "old"}, true, 0)
	}
	// Age the first batch past its TTL without advancing the live ones.
	now = now.Add(2 * time.Hour)
	for i := range maxCacheEntries - expired {
		c.put(fmt.Sprintf("live-%d:80", i), Endpoint{Pod: "new"}, true, 0)
	}

	// One more insertion reaches the bound and forces eviction. The expired
	// batch goes first, so no live entry is dropped even though one would have
	// to be if only the oldest entry were evicted.
	c.put("trigger:80", Endpoint{Pod: "newer"}, true, 0)

	c.mu.Lock()
	size := len(c.entries)
	_, expiredKept := c.entries["exp-0:80"]
	_, oldestLive := c.entries["live-0:80"]
	c.mu.Unlock()

	want := maxCacheEntries - expired + 1
	if size != want {
		t.Errorf("cache size = %d, want %d", size, want)
	}
	if expiredKept {
		t.Error("an expired entry survived eviction; expiry is not swept before the oldest entry")
	}
	if !oldestLive {
		t.Error("the oldest live entry was dropped while expired entries remained")
	}
}
