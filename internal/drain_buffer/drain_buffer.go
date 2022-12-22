package drainbuffer

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DrainBuffer will store information about the last sucessful drains
type DrainBuffer interface {
	// Register is adding the given group to the internal store
	Register(groups.GroupKey, time.Duration)
	// NextDrain returns the next possible drain time for the given group
	NextDrain(groups.GroupKey, time.Duration) time.Time
}

// drainCache is cache used internally by the drain buffer
type drainCache map[string]drainCacheEntry

// drainCacheEntry stores information about specific cache entries
type drainCacheEntry struct {
	LastDrain   time.Time     `json:"last_drain"`
	DrainBuffer time.Duration `json:"drain_buffer"`
}

type drainBufferImpl struct {
	sync.RWMutex
	clock     clock.Clock
	persistor Persistor[drainCache]
	cache     drainCache
	logger    *logr.Logger
}

// NewDrainBuffer returns a new instance of a drain buffer
func NewDrainBuffer(ctx context.Context, client client.Client, clock clock.Clock, logger *logr.Logger, name, namespace string) (DrainBuffer, error) {
	persistor := NewConfigMapPersistor[drainCache](client, name, namespace)
	// load existing cache from the persistence layer
	cache, exist, err := persistor.Load()
	if err != nil {
		return nil, err
	}
	if !exist {
		cache = &drainCache{}
	}

	drainBuffer := &drainBufferImpl{
		clock:     clock,
		persistor: persistor,
		cache:     *cache,
		logger:    logger,
	}

	go drainBuffer.persistenceLoop(ctx)

	return drainBuffer, nil
}

func (buffer *drainBufferImpl) Register(key groups.GroupKey, drainBuffer time.Duration) {
	buffer.Lock()
	defer buffer.Unlock()

	buffer.cache[string(key)] = drainCacheEntry{
		LastDrain:   buffer.clock.Now(),
		DrainBuffer: drainBuffer,
	}
}

func (buffer *drainBufferImpl) NextDrain(key groups.GroupKey, drainBuffer time.Duration) time.Time {
	buffer.RLock()
	defer buffer.RUnlock()

	entry, ok := buffer.cache[string(key)]
	if !ok {
		return time.Time{}
	}
	// TODO compare drain buffer and update if not the same
	return entry.LastDrain.Add(drainBuffer)
}

func (buffer *drainBufferImpl) cleanupCache() {
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
		if err := buffer.persist(); err != nil {
			buffer.logger.Error(err, "failed to persist drain buffer cache")
		}
	}, time.Second*20)
}

func (buffer *drainBufferImpl) persist() error {
	buffer.cleanupCache()
	return buffer.persistor.Persist(&buffer.cache)
}
