package drain_runner

import (
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Config configuration passed to the drain runner
type Config struct {
	logger     logr.Logger
	kubeClient client.Client
	clock      clock.Clock
	retryWall  drain.RetryWall
	drainer    kubernetes.Drainer

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
	return nil
}

func (conf *Config) WithKubeClient(client client.Client) *Config {
	conf.kubeClient = client
	return conf
}

func (conf *Config) WithLogger(logger logr.Logger) *Config {
	conf.logger = logger
	return conf
}

// WithPreprocessor will attach the given preprocessor to the configuration
func (conf *Config) WithPreprocessor(pre DrainPreprozessor) *Config {
	conf.preprocessors = append(conf.preprocessors, pre)
	return conf
}

// WithRerun will override the default rerun configuration
func (conf *Config) WithRerun(rerun time.Duration) *Config {
	conf.rerunEvery = rerun
	return conf
}

func (conf *Config) WithClock(c clock.Clock) *Config {
	conf.clock = c
	return conf
}

func (conf *Config) WithRetryWall(wall drain.RetryWall) *Config {
	conf.retryWall = wall
	return conf
}

func (conf *Config) WithDrainer(drainer kubernetes.Drainer) *Config {
	conf.drainer = drainer
	return conf
}
