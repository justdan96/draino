package drainer

import (
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DrainRunnerConfig configuration passed to the drain runner
type DrainRunnerConfig struct {
	preprocessors []DrainPreprozessor
	rerunEvery    time.Duration
	drainTimeout  time.Duration
}

// NewDrainRunnerConfig returns a pointer to a new drain runner configuration
func NewDrainRunnerConfig() *DrainRunnerConfig {
	return &DrainRunnerConfig{
		preprocessors: make([]DrainPreprozessor, 0),
		rerunEvery:    time.Second,
		drainTimeout:  time.Minute,
	}
}

// WithPreprocessor will attach the given preprocessor to the configuration
func (conf *DrainRunnerConfig) WithPreprocessor(pre DrainPreprozessor) *DrainRunnerConfig {
	conf.preprocessors = append(conf.preprocessors, pre)
	return conf
}

// WithDrainTimeout will override the default drain timeout configuration
func (conf *DrainRunnerConfig) WithDrainTimeout(timeout time.Duration) *DrainRunnerConfig {
	conf.drainTimeout = timeout
	return conf
}

// WithRerun will override the default rerun configuration
func (conf *DrainRunnerConfig) WithRerun(rerun time.Duration) *DrainRunnerConfig {
	conf.rerunEvery = rerun
	return conf
}

// DrainRunnerFactory can create new instances of drain runners
type DrainRunnerFactory struct {
	client client.Client
	logger logr.Logger
	clock  clock.Clock

	groupKeyGetter  groups.GroupKeyGetter
	drainRunnerConf *DrainRunnerConfig
}

func NewFactory(client client.Client, logger logr.Logger, groupKeyGetter groups.GroupKeyGetter, conf *DrainRunnerConfig) groups.RunnerFactory {
	return &DrainRunnerFactory{
		client:          client,
		logger:          logger,
		clock:           clock.RealClock{},
		groupKeyGetter:  groupKeyGetter,
		drainRunnerConf: conf,
	}
}

func (factory *DrainRunnerFactory) BuildRunner() groups.Runner {
	return &drainRunner{
		client: factory.client,
		logger: factory.logger,
		clock:  factory.clock,
		conf:   factory.drainRunnerConf,
	}
}

func (factory *DrainRunnerFactory) GroupKeyGetter() groups.GroupKeyGetter {
	return factory.groupKeyGetter
}
