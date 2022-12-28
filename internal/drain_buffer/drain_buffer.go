package drainbuffer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
)

// DrainBuffer will store information about the last sucessful drains
type DrainBuffer interface {
	// StoreSuccessfulDrain is adding the given group to the internal store
	StoreSuccessfulDrain(groups.GroupKey, time.Duration) error
	// NextDrain returns the next possible drain time for the given group
	// It will return a zero time if the next drain can be done immediately
	NextDrain(groups.GroupKey) (time.Time, error)
}

// drainCache is cache used internally by the drain buffer
type drainCache map[groups.GroupKey]drainCacheEntry

// drainCacheEntry stores information about specific cache entries
type drainCacheEntry struct {
	LastDrain   time.Time     `json:"last_drain"`
	DrainBuffer time.Duration `json:"drain_buffer"`
}

type drainBufferImpl struct {
	sync.RWMutex
	isInitialized bool
	clock         clock.Clock
	persistor     Persistor
	cache         drainCache
	logger        *logr.Logger
}

// NewDrainBuffer returns a new instance of a drain buffer
func NewDrainBuffer(ctx context.Context, persistor Persistor, clock clock.Clock, logger logr.Logger) DrainBuffer {
	drainBuffer := &drainBufferImpl{
		isInitialized: false,
		clock:         clock,
		persistor:     persistor,
		cache:         drainCache{},
		logger:        &logger,
	}

	go drainBuffer.persistenceLoop(ctx)

	return drainBuffer
}

func (buffer *drainBufferImpl) StoreSuccessfulDrain(key groups.GroupKey, drainBuffer time.Duration) error {
	if err := buffer.initialize(); err != nil {
		return err
	}

	buffer.Lock()
	defer buffer.Unlock()

	buffer.cache[key] = drainCacheEntry{
		LastDrain:   buffer.clock.Now(),
		DrainBuffer: drainBuffer,
	}

	return nil
}

func (buffer *drainBufferImpl) NextDrain(key groups.GroupKey) (time.Time, error) {
	if err := buffer.initialize(); err != nil {
		return time.Time{}, err
	}

	buffer.RLock()
	defer buffer.RUnlock()

	entry, ok := buffer.cache[key]
	if !ok {
		return time.Time{}, nil
	}
	return entry.LastDrain.Add(entry.DrainBuffer), nil
}

func (buffer *drainBufferImpl) initialize() error {
	if buffer.isInitialized {
		return nil
	}

	buffer.Lock()
	defer buffer.Unlock()

	data, exist, err := buffer.persistor.Load()
	if err != nil {
		return err
	}
	if exist {
		if err := json.Unmarshal(data, &buffer.cache); err != nil {
			return err
		}
	}

	buffer.isInitialized = true
	return nil
}

func (buffer *drainBufferImpl) cleanupCache() {
	if !buffer.isInitialized {
		return
	}

	buffer.Lock()
	defer buffer.Unlock()

	for key, entry := range buffer.cache {
		until := entry.LastDrain.Add(entry.DrainBuffer)
		if until.Before(buffer.clock.Now()) {
			delete(buffer.cache, key)
		}
	}
}

func (buffer *drainBufferImpl) persistenceLoop(ctx context.Context) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if !buffer.isInitialized {
			return
		}
		if err := buffer.cleanupAndPersist(); err != nil {
			buffer.logger.Error(err, "failed to persist drain buffer cache")
		}
	}, time.Second*20)
}

func (buffer *drainBufferImpl) cleanupAndPersist() error {
	if !buffer.isInitialized {
		return nil
	}

	buffer.cleanupCache()

	// The lock has to be acquired after the cleanup, because otherwise we'll create a deadlock
	buffer.RLock()
	defer buffer.RUnlock()

	data, err := json.Marshal(buffer.cache)
	if err != nil {
		return err
	}

	return buffer.persistor.Persist(data)
}
