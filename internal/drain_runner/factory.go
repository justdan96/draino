package drain_runner

import (
	"github.com/planetlabs/draino/internal/groups"
)

// DrainRunnerFactory can create new instances of drain runners
type DrainRunnerFactory struct {
	conf *Config
}

func NewFactory(withOptions ...WithOption) (groups.RunnerFactory, error) {
	conf := NewConfig()
	for _, opt := range withOptions {
		opt(conf)
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}

	return &DrainRunnerFactory{conf: conf}, nil
}

func (factory *DrainRunnerFactory) BuildRunner() groups.Runner {
	return &drainRunner{
		client:    factory.conf.kubeClient,
		logger:    factory.conf.logger,
		clock:     factory.conf.clock,
		retryWall: factory.conf.retryWall,
		drainer:   factory.conf.drainer,
		runEvery:  factory.conf.rerunEvery,

		preprocessors: factory.conf.preprocessors,
	}
}
