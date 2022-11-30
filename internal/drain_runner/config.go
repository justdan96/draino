package drain_runner

import (
	"time"
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
