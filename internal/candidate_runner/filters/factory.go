package filters

// FilterFactory factory to build a composite filter with all the known filters
type FilterFactory struct {
	conf *Config
}

// NewFactory will return a new filter factory
// It will return an error if the given configuration is invalid or incomplete.
func NewFactory(withOptions ...WithOption) (*FilterFactory, error) {
	conf := NewConfig()
	for _, opt := range withOptions {
		opt(conf)
	}

	if err := conf.Validate(); err != nil {
		return nil, err
	}
	return &FilterFactory{conf: conf}, nil
}

func (factory *FilterFactory) Builder(group string) Filter {
	f := &CompositeFilter{
		logger: factory.conf.logger.WithName("composite_filter").WithValues("group", group),
	}

	f.filters = []Filter{
		NewNodeWithConditionFilter(group, factory.conf.globalConfig.SuppliedConditions),
		NewNodeWithLabelFilter(group, factory.conf.nodeLabelFilterFunc),
		NewPodFilter(*factory.conf.logger, group, factory.conf.cordonFilter, factory.conf.objectsStore),
		NewRetryWallFilter(group, factory.conf.clock, factory.conf.retryWall),
		NewNodeTerminatingFilter(group),
	}
	return f
}
