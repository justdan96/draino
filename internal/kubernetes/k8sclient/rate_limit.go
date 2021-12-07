package k8sclient

import (
	"context"
	"reflect"
	"sync"
	"unsafe"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/time/rate"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

var (
	tagClientName                   tag.Key
	k8sClientTokensRemainingMeasure *stats.Float64Measure
)

func init() {
	var err error
	tagClientName, err = tag.NewKey("client_name")
	if err != nil {
		panic(err)
	}
	k8sClientTokensMetricName := "k8s_client_tokens_remaining"
	description := "Number of remaining tokens in the k8s client rate limiter"
	k8sClientTokensRemainingMeasure = stats.Float64(k8sClientTokensMetricName, description, stats.UnitDimensionless)
	v := &view.View{
		Name:        k8sClientTokensMetricName,
		Measure:     k8sClientTokensRemainingMeasure,
		Description: description,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{tagClientName},
	}
	view.Register(v)
}

func DecorateWithRateLimiter(config *rest.Config, name string) {
	// defaulting and inner rate imiter creation copied from
	// https://github.com/kubernetes/kubernetes/blob/0153febd9f0098d4b8d0d484927710eaf899ef40/staging/src/k8s.io/client-go/rest/config.go#L354
	qps := config.QPS
	if config.QPS == 0.0 {
		qps = rest.DefaultQPS
	}
	burst := config.Burst
	if config.Burst == 0 {
		burst = rest.DefaultBurst
	}
	rateLimiter := flowcontrol.NewTokenBucketRateLimiter(qps, burst)
	config.RateLimiter = NewRateLimiterWithMetric(name, rateLimiter)
}

type rateLimiterWithMetric struct {
	name        string
	rateLimiter flowcontrol.RateLimiter
	limiter     *rate.Limiter
	mutex       sync.Mutex
}

var _ flowcontrol.RateLimiter = &rateLimiterWithMetric{}

func NewRateLimiterWithMetric(name string, rateLimiter flowcontrol.RateLimiter) *rateLimiterWithMetric {
	// reflection hacks from https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields/43918797#43918797
	limiterField := reflect.Indirect(reflect.ValueOf(rateLimiter)).FieldByName("limiter")
	limiterField = reflect.NewAt(limiterField.Type(), unsafe.Pointer(limiterField.UnsafeAddr())).Elem()
	limiter := limiterField.Interface().(*rate.Limiter)
	return &rateLimiterWithMetric{
		name:        name,
		rateLimiter: rateLimiter,
		limiter:     limiter,
	}
}

func (r *rateLimiterWithMetric) TryAccept() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer r.updateMetric()
	return r.rateLimiter.TryAccept()
}

func (r *rateLimiterWithMetric) Accept() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer r.updateMetric()
	r.rateLimiter.Accept()
}

func (r *rateLimiterWithMetric) Stop() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer r.updateMetric()
	r.rateLimiter.Stop()
}

func (r *rateLimiterWithMetric) QPS() float32 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.rateLimiter.QPS()
}

func (r *rateLimiterWithMetric) updateMetric() {
	tokens := r.tokens()
	allTags, _ := tag.New(context.Background(), tag.Upsert(tagClientName, r.name))
	stats.Record(allTags, k8sClientTokensRemainingMeasure.M(tokens))
}

func (r *rateLimiterWithMetric) tokens() float64 {
	// tokens is not exposed, filed https://github.com/golang/go/issues/50035
	tokensField := reflect.Indirect(reflect.ValueOf(r.limiter)).FieldByName("tokens")
	tokensField = reflect.NewAt(tokensField.Type(), unsafe.Pointer(tokensField.UnsafeAddr())).Elem()
	tokens := tokensField.Interface().(float64)
	return tokens
}
