package drain

import (
	"context"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type FakeSimulatorOptions struct {
	CleanupDuration *time.Duration
	CacheTTL        *time.Duration

	Objects   []runtime.Object
	PodFilter kubernetes.PodFilterFunc
}

func (opts *FakeSimulatorOptions) applyDefaults() {
	if opts.CacheTTL == nil {
		opts.CacheTTL = utils.DurationPtr(time.Minute)
	}
	if opts.CleanupDuration == nil {
		opts.CleanupDuration = utils.DurationPtr(10 * time.Second)
	}
}

func NewFakeDrainSimulator(ch chan struct{}, opts *FakeSimulatorOptions) (DrainSimulator, func(), error) {
	opts.applyDefaults()

	fakeIndexer, err := index.NewFakeIndexer(ch, opts.Objects)
	if err != nil {
		return nil, func() {}, err
	}

	fakeClient := fake.NewFakeClient(opts.Objects...)
	store, closeFn := kubernetes.RunStoreForTest(context.Background(), fakeclient.NewSimpleClientset(opts.Objects...))

	simulator := &drainSimulatorImpl{
		store:          store,
		podIndexer:     fakeIndexer,
		pdbIndexer:     fakeIndexer,
		client:         fakeClient,
		podResultCache: utils.NewTTLCache[simulationResult](*opts.CacheTTL, *opts.CleanupDuration),
		skipPodFilter:  opts.PodFilter,
	}

	return simulator, closeFn, nil
}
