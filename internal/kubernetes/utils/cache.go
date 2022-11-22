package utils

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
)

var _ TTLCache[string] = &ttlCacheImpl[string]{}

type TTLCache[T any] interface {
	// StartCleanupLoop will run a continuous loop that is executing a cleanup every now and then
	StartCleanupLoop(ctx context.Context)
	// Cleanup will cleanup the internal cache and remove outdated elements
	Cleanup()
	// Add adds the given element to the cache
	Add(string, T)
	// Get returns the element of the given key
	// The boolean will be false if there is no element with this key in the cache
	Get(string) (T, bool)
}

type ttlCacheImpl[T any] struct {
	ttl             time.Duration
	cleanupDuration time.Duration

	cache cache.ThreadSafeStore
}

// ttlEntry is used to store the given element in the thread safe cache
type ttlEntry[T any] struct {
	until time.Time
	entry T
}

// NewTTLCache will create an instance of the TTLCache
func NewTTLCache[T any](ttl, cleanup time.Duration) TTLCache[T] {
	return &ttlCacheImpl[T]{
		ttl:             ttl,
		cleanupDuration: cleanup,
		cache:           cache.NewThreadSafeStore(nil, nil),
	}
}

func (c *ttlCacheImpl[T]) StartCleanupLoop(ctx context.Context) {
	wait.Until(
		c.Cleanup,
		c.cleanupDuration,
		ctx.Done(),
	)
}

func (c *ttlCacheImpl[T]) Cleanup() {
	now := time.Now()

	for _, key := range c.cache.ListKeys() {
		entry, exist := c.cache.Get(key)
		if !exist {
			continue
		}

		e := entry.(ttlEntry[T])
		if e.until.Before(now) {
			c.cache.Delete(key)
		}
	}
}

func (c *ttlCacheImpl[T]) Add(key string, val T) {
	entry := ttlEntry[T]{
		until: time.Now().Add(c.ttl),
		entry: val,
	}
	c.cache.Add(key, entry)
}

func (c *ttlCacheImpl[T]) Get(key string) (T, bool) {
	entry, exist := c.cache.Get(key)
	if !exist {
		var empty T
		return empty, false
	}

	parsed, ok := entry.(ttlEntry[T])
	if !ok {
		var empty T
		return empty, false
	}

	// Make sure that the item did not reach it's TTL
	if parsed.until.Before(time.Now()) {
		var empty T
		return empty, false
	}

	return parsed.entry, true
}
