package drain_runner

import (
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type WithOption = func(conf *Config)

// Config configuration passed to the drain runner
type Config struct {
	logger              logr.Logger
	kubeClient          client.Client
	clock               clock.Clock
	retryWall           drain.RetryWall
	drainer             kubernetes.Drainer
	sharedIndexInformer index.GetSharedIndexInformer

	preprocessors []DrainPreprozessor
	rerunEvery    time.Duration
}

// NewConfig returns a pointer to a new drain runner configuration
func NewConfig() *Config {
	return &Config{
		logger:        logr.Discard(),
		clock:         clock.RealClock{},
		preprocessors: make([]DrainPreprozessor, 0),
		rerunEvery:    time.Second,
	}
}

func (conf *Config) Validate() error {
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
	return nil
}

func WithKubeClient(client client.Client) WithOption {
	return func(conf *Config) {
		conf.kubeClient = client
	}
}

func (conf *Config) WithLogger(logger logr.Logger) WithOption {
	return func(conf *Config) {
		conf.logger = logger
	}
}

// WithPreprocessor will attach the given preprocessor to the configuration
func (conf *Config) WithPreprocessor(pre DrainPreprozessor) WithOption {
	return func(conf *Config) {
		conf.preprocessors = append(conf.preprocessors, pre)
	}
}

// WithRerun will override the default rerun configuration
func (conf *Config) WithRerun(rerun time.Duration) WithOption {
	return func(conf *Config) {
		conf.rerunEvery = rerun
	}
}

func (conf *Config) WithClock(c clock.Clock) WithOption {
	return func(conf *Config) {
		conf.clock = c
	}
}

func (conf *Config) WithRetryWall(wall drain.RetryWall) WithOption {
	return func(conf *Config) {
		conf.retryWall = wall
	}
}

func (conf *Config) WithDrainer(drainer kubernetes.Drainer) WithOption {
	return func(conf *Config) {
		conf.drainer = drainer
	}
}

func (conf *Config) WithSharedIndexInformer(inf index.GetSharedIndexInformer) WithOption {
	return func(conf *Config) {
		conf.sharedIndexInformer = inf
	}
}
