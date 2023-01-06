package limit

import (
	"context"

	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"
)

type RateLimiter interface {
	// TryAccept returns true if a token is taken immediately. Otherwise,
	// it returns false.
	TryAccept() bool
	// Wait returns nil if a token is taken before the Context is done.
	Wait(context.Context) error
}

// rateLimiterImpl is a wrapper to abstract the flowcontrol rate limiter to the other interal parts of the code
type rateLimiterImpl struct {
	rateLimiter flowcontrol.RateLimiter
}

func NewRateLimiter(clock clock.Clock, qps float32, burst int) RateLimiter {
	return &rateLimiterImpl{
		rateLimiter: flowcontrol.NewTokenBucketRateLimiterWithClock(qps, burst, clock),
	}
}

func (limit *rateLimiterImpl) Wait(ctx context.Context) error {
	return limit.rateLimiter.Wait(ctx)
}
func (limit *rateLimiterImpl) TryAccept() bool {
	return limit.rateLimiter.TryAccept()
}
