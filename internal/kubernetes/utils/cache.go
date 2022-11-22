package utils

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
)

var _ TTLCache[string] = &ttlCacheImpl[string]{}

type TTLCache[T any] interface {
	StartCleanupLoop(ctx context.Context)
	Cleanup()
	Add(string, T)
	Get(string) (T, bool)
}

type ttlCacheImpl[T any] struct {
	ttl             time.Duration
	cleanupDuration time.Duration

	cache cache.ThreadSafeStore
}

type ttlEntry[T any] struct {
	until time.Time
	entry T
}

func NewCache[T any](ttl, cleanup time.Duration) TTLCache[T] {
	return &ttlCacheImpl[T]{
		ttl:             ttl,
		cleanupDuration: cleanup,
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
