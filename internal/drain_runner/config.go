package drain_runner

import (
	"errors"
	"time"

	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	preprocessor "github.com/planetlabs/draino/internal/drain_runner/pre_processor"
	"github.com/planetlabs/draino/internal/protector"

	"github.com/go-logr/logr"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
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
	eventRecorder       kubernetes.EventRecorder
	filter              filters.Filter
	drainBuffer         drainbuffer.DrainBuffer
	nodeReplacer        *preprocessor.NodeReplacer
	suppliedCondition   []kubernetes.SuppliedCondition
	pvcProtector        protector.PVCProtector

	// With defaults
	clock         clock.Clock
	preprocessors []preprocessor.DrainPreProcessor
	rerunEvery    time.Duration

	// Options
	durationWithDrainedStatusBeforeReplacement time.Duration
}

// NewConfig returns a pointer to a new drain runner configuration
func NewConfig() *Config {
	return &Config{
		clock:         clock.RealClock{},
		preprocessors: make([]preprocessor.DrainPreProcessor, 0),
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
	if conf.eventRecorder == nil {
		return errors.New("event recorder should be set")
	}
	if conf.filter == nil {
		return errors.New("filter should be set")
	}
	if conf.drainBuffer == nil {
		return errors.New("drain buffer should be set")
	}
	if conf.nodeReplacer == nil {
		return errors.New("node replacer should be set")
	}
	if len(conf.suppliedCondition) == 0 {
		return errors.New("global config is not set")
	}
	if conf.pvcProtector == nil {
		return errors.New("pvcProtector should be set")
	}
	if conf.durationWithDrainedStatusBeforeReplacement == 0 {
		return errors.New("options should be set")
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

func WithPreprocessors(pre ...preprocessor.DrainPreProcessor) WithOption {
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

func WithEventRecorder(er kubernetes.EventRecorder) WithOption {
	return func(conf *Config) {
		conf.eventRecorder = er
	}
}

func WithFilter(f filters.Filter) WithOption {
	return func(conf *Config) {
		conf.filter = f
	}
}

func WithDrainBuffer(buffer drainbuffer.DrainBuffer) WithOption {
	return func(conf *Config) {
		conf.drainBuffer = buffer
	}
}

func WithNodeReplacer(nodeReplacer *preprocessor.NodeReplacer) WithOption {
	return func(conf *Config) {
		conf.nodeReplacer = nodeReplacer
	}
}

func WithGlobalConfig(globalConfig kubernetes.GlobalConfig) WithOption {
	return func(conf *Config) {
		conf.suppliedCondition = globalConfig.SuppliedConditions
	}
}

func WithBeforeReplacementDuration(durationWithDrainedStatusBeforeReplacement time.Duration) WithOption {
	return func(conf *Config) {
		conf.durationWithDrainedStatusBeforeReplacement = durationWithDrainedStatusBeforeReplacement
	}
}

func WithPVCProtector(pvcProtector protector.PVCProtector) WithOption {
	return func(conf *Config) {
		conf.pvcProtector = pvcProtector
	}
}
