package drain_runner

import (
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DrainRunnerFactory can create new instances of drain runners
type DrainRunnerFactory struct {
	client client.Client
	logger logr.Logger
	clock  clock.Clock

	drainRunnerConf *DrainRunnerConfig
}

func NewFactory(client client.Client, logger logr.Logger, groupKeyGetter groups.GroupKeyGetter, conf *DrainRunnerConfig) groups.RunnerFactory {
	return &DrainRunnerFactory{
		client:          client,
		logger:          logger,
		clock:           clock.RealClock{},
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
