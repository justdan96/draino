package drain_runner

import (
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/protector"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WithOption is used to pass an option to the factory
type WithOption = func(conf *Config)

// Config configuration passed to the drain runner
type Config struct {
	// Have to be set
	logger              *logr.Logger
	kubeClient          client.Client
	retryWall           drain.RetryWall
	drainer             kubernetes.Drainer
	sharedIndexInformer index.GetSharedIndexInformer
	pvProtector         protector.PVProtector

	// With defaults
	clock         clock.Clock
	preprocessors []DrainPreProzessor
	rerunEvery    time.Duration
}

// NewConfig returns a pointer to a new drain runner configuration
func NewConfig() *Config {
	return &Config{
		clock:         clock.RealClock{},
		preprocessors: make([]DrainPreProzessor, 0),
		rerunEvery:    time.Second,
	}
}

// Validate validates the configuration and will return an error in case of misconfiguration
func (conf *Config) Validate() error {
	if conf.logger == nil {
		return errors.New("logger should be set")
	}
	if conf.kubeClient == nil {
		return errors.New("kube client should be set")
	}
	if conf.retryWall == nil {
		return errors.New("retry wall should be set")
	}
	if conf.drainer == nil {
		return errors.New("drainer should be set")
	}
	if conf.sharedIndexInformer == nil {
		return errors.New("get shared index informer should be set")
	}
	if conf.pvProtector == nil {
		return errors.New("pv protector should be set")
	}

	return nil
}

func WithKubeClient(client client.Client) WithOption {
	return func(conf *Config) {
		conf.kubeClient = client
	}
}

func WithLogger(logger logr.Logger) WithOption {
	return func(conf *Config) {
		conf.logger = &logger
	}
}

func WithPreprocessors(pre ...DrainPreProzessor) WithOption {
	return func(conf *Config) {
		conf.preprocessors = append(conf.preprocessors, pre...)
	}
}

func WithRerun(rerun time.Duration) WithOption {
	return func(conf *Config) {
		conf.rerunEvery = rerun
	}
}

func WithClock(c clock.Clock) WithOption {
	return func(conf *Config) {
		conf.clock = c
	}
}

func WithRetryWall(wall drain.RetryWall) WithOption {
	return func(conf *Config) {
		conf.retryWall = wall
	}
}

func WithDrainer(drainer kubernetes.Drainer) WithOption {
	return func(conf *Config) {
		conf.drainer = drainer
	}
}

func WithSharedIndexInformer(inf index.GetSharedIndexInformer) WithOption {
	return func(conf *Config) {
		conf.sharedIndexInformer = inf
	}
}

func WithPVProtector(protector protector.PVProtector) WithOption {
	return func(conf *Config) {
		conf.pvProtector = protector
	}
}
