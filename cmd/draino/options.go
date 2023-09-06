package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/spf13/pflag"

	circuitbreaker "github.com/planetlabs/draino/internal/circuit_breaker"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
)

const (
	DefaultPreprovisioningTimeout      = 1*time.Hour + 20*time.Minute
	DefaultPreprovisioningCheckPeriod  = 30 * time.Second
	DefaultSchedulingRetryBackoffDelay = 23 * time.Minute
)

// Options collects the program options/parameters
type Options struct {
	noLegacyNodeHandler         bool
	debug                       bool
	listen                      string
	kubecfg                     string
	apiserver                   string
	dryRun                      bool
	minEvictionTimeout          time.Duration
	evictionHeadroom            time.Duration
	drainBuffer                 time.Duration
	drainBufferConfigMapName    string
	schedulingRetryBackoffDelay time.Duration
	nodeLabels                  []string
	nodeLabelsExpr              string
	nodeAndPodsExpr             string

	// Eviction filtering flags
	skipDrain                 bool
	doNotEvictPodControlledBy []string
	evictLocalStoragePods     bool
	protectedPodAnnotations   []string
	drainGroupLabelKey        string

	// Candidate filtering flags
	doNotCandidatePodControlledBy          []string
	candidateLocalStoragePods              bool
	excludeStatefulSetOnNodeWithoutStorage bool
	candidateProtectedPodAnnotations       []string

	maxNotReadyNodes          []string
	maxNotReadyNodesFunctions map[string]kubernetes.ComputeBlockStateFunctionFactory
	maxNotReadyNodesPeriod    time.Duration

	maxPendingPods          []string
	maxPendingPodsFunctions map[string]kubernetes.ComputeBlockStateFunctionFactory
	maxPendingPodsPeriod    time.Duration

	maxDrainAttemptsBeforeFail int

	// Pod Opt-in flags
	optInPodAnnotations      []string
	shortLivedPodAnnotations []string

	// NodeReplacement limiter flags
	maxNodeReplacementPerHour int
	durationBeforeReplacement time.Duration

	// Preprovisioning flags
	preprovisioningTimeout            time.Duration
	preprovisioningCheckPeriod        time.Duration
	preprovisioningActivatedByDefault bool
	preActivityDefaultTimeout         time.Duration

	// PV/PVC management
	storageClassesAllowingVolumeDeletion []string
	pvcManagementByDefault               bool

	// Drain runner rate limiting
	drainRateLimitQPS   float32
	drainRateLimitBurst int

	waitBeforeDraining time.Duration

	// Which ratio of the overall kube client rate limiting should be used by the drain simulation
	simulationRateLimitingRatio float32

	// events generation
	eventAggregationPeriod        time.Duration
	excludedPodsPerNodeEstimation int
	logEvents                     bool

	//circuit breaker
	monitorCircuitBreakerCheckPeriod time.Duration
	monitorCircuitBreakerMonitorTags map[string]string
	circuitBreakerRateLimitQPS       float32

	configName          string
	resetScopeLabel     bool
	scopeAnalysisPeriod time.Duration

	groupRunnerPeriod       time.Duration
	podWarmupDelayExtension time.Duration

	klogVerbosity int32

	conditions         []string
	suppliedConditions []kubernetes.SuppliedCondition
}

func optionsFromFlags() (*Options, *pflag.FlagSet) {
	var (
		fs  pflag.FlagSet
		opt Options
	)
	fs.BoolVar(&opt.debug, "debug", false, "Run with debug logging.")
	fs.BoolVar(&opt.dryRun, "dry-run", false, "Emit an event without tainting or draining matching nodes.")
	fs.BoolVar(&opt.skipDrain, "skip-drain", false, "Whether to skip draining nodes after tainting.")
	fs.BoolVar(&opt.evictLocalStoragePods, "evict-emptydir-pods", false, "Evict pods with local storage, i.e. with emptyDir volumes.")
	fs.BoolVar(&opt.candidateLocalStoragePods, "candidate-emptydir-pods", true, "Evict pods with local storage, i.e. with emptyDir volumes.")
	fs.BoolVar(&opt.preprovisioningActivatedByDefault, "preprovisioning-by-default", false, "Set this flag to activate pre-provisioning by default for all nodes")
	fs.BoolVar(&opt.pvcManagementByDefault, "pvc-management-by-default", false, "PVC management is automatically activated for a workload that do not use eviction++")
	fs.BoolVar(&opt.resetScopeLabel, "reset-config-labels", false, "Reset the scope label on the nodes")
	fs.BoolVar(&opt.noLegacyNodeHandler, "no-legacy-node-handler", false, "Deactivate draino legacy node handler")
	fs.BoolVar(&opt.logEvents, "log-events", true, "Indicate if events sent to kubernetes should also be logged")
	fs.BoolVar(&opt.excludeStatefulSetOnNodeWithoutStorage, "exclude-sts-on-node-without-storage", true, "To ensure backward compatibility with draino v1, we have to exclude pod of STS running on node without local-storage")

	fs.DurationVar(&opt.minEvictionTimeout, "min-eviction-timeout", kubernetes.DefaultMinEvictionTimeout, "Minimum time we wait to evict a pod. The pod terminationGracePeriod will be used if it is bigger.")
	fs.DurationVar(&opt.evictionHeadroom, "eviction-headroom", kubernetes.DefaultEvictionOverhead, "Additional time to wait after a pod's termination grace period for it to have been deleted.")
	fs.DurationVar(&opt.drainBuffer, "drain-buffer", kubernetes.DefaultDrainBuffer, "Delay to respect between end of previous drain (success or error) and a new attempt within a drain-group.")
	fs.StringVar(&opt.drainBufferConfigMapName, "drain-buffer-configmap-name", "", "The name of the configmap used to persist the drain-buffer values. Default will be draino-<config-name>-drain-buffer.")
	fs.DurationVar(&opt.schedulingRetryBackoffDelay, "retry-backoff-delay", DefaultSchedulingRetryBackoffDelay, "Additional delay to add between retry schedules.")
	fs.DurationVar(&opt.maxNotReadyNodesPeriod, "max-notready-nodes-period", kubernetes.DefaultMaxNotReadyNodesPeriod, "Polling period to check all nodes readiness")
	fs.DurationVar(&opt.maxPendingPodsPeriod, "max-pending-pods-period", kubernetes.DefaultMaxPendingPodsPeriod, "Polling period to check volume of pending pods")
	fs.DurationVar(&opt.durationBeforeReplacement, "duration-before-replacement", kubernetes.DefaultDurationBeforeReplacement, "Max duration we are waiting for a node with Completed drain status to be removed before asking for replacement.")
	fs.DurationVar(&opt.preprovisioningTimeout, "preprovisioning-timeout", DefaultPreprovisioningTimeout, "Timeout for a node to be preprovisioned before draining")
	fs.DurationVar(&opt.preprovisioningCheckPeriod, "preprovisioning-check-period", DefaultPreprovisioningCheckPeriod, "Period to check if a node has been preprovisioned")
	fs.DurationVar(&opt.scopeAnalysisPeriod, "scope-analysis-period", 5*time.Minute, "Period to run the scope analysis and generate metric")
	fs.DurationVar(&opt.groupRunnerPeriod, "group-runner-period", 10*time.Second, "Period for running the group runner")
	fs.DurationVar(&opt.podWarmupDelayExtension, "pod-warmup-delay-extension", 30*time.Second, "Extra delay given to the pod to complete is warmup phase (all containers have passed their startProbes)")
	fs.DurationVar(&opt.eventAggregationPeriod, "event-aggregation-period", 15*time.Minute, "Period for event generation on kubernetes object.")
	fs.DurationVar(&opt.waitBeforeDraining, "wait-before-draining", 30*time.Second, "Time to wait between moving a node in candidate status and starting the actual drain.")
	fs.DurationVar(&opt.preActivityDefaultTimeout, "pre-activity-default-timeout", 10*time.Minute, "Default duration to wait, for a pre activity to finish, before aborting the drain. This can be overridden by an annotation.")
	fs.DurationVar(&opt.monitorCircuitBreakerCheckPeriod, "monitor-check-circuit-breaker-period", 1*time.Minute, "Period for checking the monitors associated with circuit breakers.")

	fs.StringSliceVar(&opt.nodeLabels, "node-label", []string{}, "(Deprecated) Nodes with this label will be eligible for tainting and draining. May be specified multiple times")
	fs.StringSliceVar(&opt.doNotEvictPodControlledBy, "do-not-evict-pod-controlled-by", []string{"", kubernetes.KindStatefulSet, kubernetes.KindDaemonSet},
		"Do not evict pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times: kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1")
	fs.StringSliceVar(&opt.protectedPodAnnotations, "protected-pod-annotation", []string{}, "Protect pods with this annotation from eviction. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.doNotCandidatePodControlledBy, "do-not-cordon-pod-controlled-by", []string{"", kubernetes.KindStatefulSet}, "Do not make candidate nodes hosting pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times. kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1")
	fs.StringSliceVar(&opt.candidateProtectedPodAnnotations, "cordon-protected-pod-annotation", []string{}, "Protect nodes hosting pods with this annotation from being candidate. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.maxNotReadyNodes, "max-notready-nodes", []string{}, "Maximum number of NotReady nodes in the cluster. When exceeding this value draino stop taking actions. (Value|Value%)")
	fs.StringSliceVar(&opt.maxPendingPods, "max-pending-pods", []string{}, "Maximum number of Pending Pods in the cluster. When exceeding this value draino stop taking actions. (Value|Value%)")
	fs.StringSliceVar(&opt.optInPodAnnotations, "opt-in-pod-annotation", []string{}, "Pod filtering out is ignored if the pod holds one of these annotations. In a way, this makes the pod directly eligible for draino eviction. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.shortLivedPodAnnotations, "short-lived-pod-annotation", []string{}, "Pod that have a short live, just like job; we prefer let them run till the end instead of evicting them; node is cordon. May be specified multiple times. KEY[=VALUE]")
	fs.StringSliceVar(&opt.storageClassesAllowingVolumeDeletion, "storage-class-allows-pv-deletion", []string{}, "Storage class for which persistent volume (and associated claim) deletion is allowed. May be specified multiple times.")

	fs.StringVar(&opt.nodeLabelsExpr, "node-label-expr", "", "Nodes that match this expression will be eligible for tainting and draining.")
	fs.StringVar(&opt.nodeAndPodsExpr, "node-and-pods-expr", "", "(For now, only log diff with other filters) If a node and its pods match this expression, the node is eligible for tainting and draining. If not, the node is eligible unless any of its pods belongs to a statefulset, and neither the pod nor the statefulset is annotated with node-lifecycle.datadoghq.com/enabled=true.")
	fs.StringVar(&opt.listen, "listen", ":10002", "Address at which to expose /metrics and /healthz.")
	fs.StringVar(&opt.kubecfg, "kubeconfig", "", "Path to kubeconfig file. Leave unset to use in-cluster config.")
	fs.StringVar(&opt.apiserver, "master", "", "Address of Kubernetes API server. Leave unset to use in-cluster config.")
	fs.StringVar(&opt.drainGroupLabelKey, "drain-group-labels", "", "Comma separated list of label keys to be used to form draining groups. KEY1,KEY2,...")
	fs.StringVar(&opt.configName, "config-name", "", "Name of the draino configuration")

	fs.StringToStringVar(&opt.monitorCircuitBreakerMonitorTags, "circuit-breaker-monitor-tags", map[string]string{"cluster-autoscaler": "draino-circuit-breaker,cluster-autoscaler"}, "tags on monitors used for circuit breakers based on monitors. The keys are circuit breaker names, and the values are comma-separated lists of tags. Repeat the flag for multiple key-value pairs, i.e., multiple circuit breakers.")

	// We are using some values with json content, so don't use StringSlice: https://github.com/spf13/pflag/issues/370
	fs.StringArrayVar(&opt.conditions, "node-conditions", nil, "A map from condition ID to node condition, when any of these conditions are true a node will be eligible for drain.")

	fs.IntVar(&opt.maxDrainAttemptsBeforeFail, "max-drain-attempts-before-fail", 8, "Maximum number of failed drain attempts before giving-up on draining the node.")
	fs.IntVar(&opt.maxNodeReplacementPerHour, "max-node-replacement-per-hour", 2, "Maximum number of nodes per hour for which draino can ask replacement.")
	fs.IntVar(&opt.excludedPodsPerNodeEstimation, "excluded-pod-per-node-estimation", 5, "Estimation of the number of pods that should be excluded from nodes. Used to compute some event cache size.")
	fs.Int32Var(&opt.klogVerbosity, "klog-verbosity", 4, "Verbosity to run klog at")
	// The default is allowing up to 50 drains within one minute
	fs.Float32Var(&opt.drainRateLimitQPS, "drain-rate-limit-qps", kubernetes.DefaultDrainRateLimitQPS, "Maximum number of node drains per seconds per condition")
	fs.IntVar(&opt.drainRateLimitBurst, "drain-rate-limit-burst", kubernetes.DefaultDrainRateLimitBurst, "Maximum number of parallel drains within a timeframe")
	fs.Float32Var(&opt.simulationRateLimitingRatio, "drain-sim-rate-limit-ratio", 0.7, "Which ratio of the overall kube client rate limiting should be used by the drain simulation. 1.0 means that it will use the same.")
	fs.Float32Var(&opt.circuitBreakerRateLimitQPS, "circuit-breaker-rate-limit-qps", circuitbreaker.DefaultRateLimitQPS, "Maximum number of drain attempts when circuit breaker is half-open")

	return &opt, &fs
}

func (o *Options) Validate() error {
	if o.configName == "" {
		return fmt.Errorf("--config-name must be defined and not empty")
	}
	var err error

	// If the drain buffer config name is not set, we'll reuse the configName
	if o.drainBufferConfigMapName == "" {
		o.drainBufferConfigMapName = fmt.Sprintf("draino-%s-drain-buffer", o.configName)
	}

	// NotReady Nodes and NotReady Pods
	factoryComputeBlockStateForNodes := func(max int, percent bool) kubernetes.ComputeBlockStateFunctionFactory {
		return func(idx *index.Indexer, l logr.Logger) kubernetes.ComputeBlockStateFunction {
			return kubernetes.MaxNotReadyNodesCheckFunc(max, percent, idx, l)
		}
	}
	o.maxNotReadyNodesFunctions = map[string]kubernetes.ComputeBlockStateFunctionFactory{}
	for _, p := range o.maxNotReadyNodes {
		max, percent, parseErr := kubernetes.ParseMaxInParameter(p)
		if parseErr != nil {
			return fmt.Errorf("cannot parse 'max-notready-nodes' argument, %#v", parseErr)
		}
		o.maxNotReadyNodesFunctions[p] = factoryComputeBlockStateForNodes(max, percent)
	}
	factoryComputeBlockStateForPods := func(max int, percent bool) kubernetes.ComputeBlockStateFunctionFactory {
		return func(idx *index.Indexer, l logr.Logger) kubernetes.ComputeBlockStateFunction {
			return kubernetes.MaxPendingPodsCheckFunc(max, percent, idx, l)
		}
	}
	o.maxPendingPodsFunctions = map[string]kubernetes.ComputeBlockStateFunctionFactory{}
	for _, p := range o.maxPendingPods {
		max, percent, parseErr := kubernetes.ParseMaxInParameter(p)
		if parseErr != nil {
			return fmt.Errorf("cannot parse 'max-pending-pods' argument, %#v", parseErr)
		}
		o.maxPendingPodsFunctions[p] = factoryComputeBlockStateForPods(max, percent)
	}

	// Check that conditions are defined and well formatted
	if len(o.conditions) == 0 {
		return fmt.Errorf("no condition defined")
	}
	// Sanitize user input
	sort.Strings(o.conditions)
	if o.suppliedConditions, err = kubernetes.ParseConditions(o.conditions); err != nil {
		return fmt.Errorf("one of the conditions is not correctly formatted: %#v", err)
	}
	if o.groupRunnerPeriod < time.Second {
		return fmt.Errorf("group runner period should be at least 1s")
	}
	if o.podWarmupDelayExtension < time.Second {
		return fmt.Errorf("pod warmup delay extension should be at least 1s")
	}

	if o.monitorCircuitBreakerCheckPeriod < 30*time.Second {
		return fmt.Errorf("monitor polling for circuit breaker seems to be too aggressive")
	}
	for k, tags := range o.monitorCircuitBreakerMonitorTags {
		if k == "" {
			return fmt.Errorf("circuit breaker cannot have an empty name")
		}
		if len(tags) == 0 {
			return fmt.Errorf("circuit breaker (%s) cannot have an empty tag list", k)
		}
		for _, t := range strings.Split(tags, ",") {
			if t == "" {
				return fmt.Errorf("circuit breaker (%s) cannot have an empty tag", k)
			}
		}
	}
	return nil
}
