/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"github.com/DataDog/compute-go/kubeclient"
	"github.com/DataDog/compute-go/version"
	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	protector "github.com/planetlabs/draino/internal/protector"
	"github.com/spf13/cobra"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/utils/clock"
	"net/http"
	_ "net/http/pprof"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sort"
	"strings"
	"time"

	"github.com/DataDog/compute-go/controllerruntime"
	"github.com/DataDog/compute-go/infraparameters"
	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	drainoklog "github.com/planetlabs/draino/internal/kubernetes/klog"
)

// Default leader election settings.
const (
	DefaultLeaderElectionLeaseDuration = 15 * time.Second
	DefaultLeaderElectionRenewDeadline = 10 * time.Second
	DefaultLeaderElectionRetryPeriod   = 2 * time.Second
)

func main() {
	tracer.Start(
		tracer.WithService("draino"),
	)
	defer tracer.Stop()
	mux := httptrace.NewServeMux()
	go http.ListenAndServe("localhost:8085", mux) // for go profiler

	// Read application flags
	cfg, fs := controllerruntime.ConfigFromFlags(false, false)
	options, optFlags := optionsFromFlags()
	fs.AddFlagSet(optFlags)

	root := &cobra.Command{
		Short:        "disruption-budget-manager",
		Long:         "disruption-budget-manager",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(version.NewCommand())

	root.RunE = func(cmd *cobra.Command, args []string) error {

		if errOptions := options.Validate(); errOptions != nil {
			return errOptions
		}

		log, err := zap.NewProduction()
		if err != nil {
			return err
		}
		if options.debug {
			log, err = zap.NewDevelopment()
		}
		drainoklog.InitializeKlog(options.klogVerbosity)
		drainoklog.RedirectToLogger(log)

		defer log.Sync() // nolint:errcheck // no check required on program exit

		DrainoLegacyMetrics(options, log)

		cs, err2 := GetKubernetesClientSet(&cfg.KubeClientConfig)
		if err2 != nil {
			return err2
		}

		// use a Go context so we can tell the leaderelection and other pieces when we want to step down
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pods := kubernetes.NewPodWatch(ctx, cs)
		statefulSets := kubernetes.NewStatefulsetWatch(ctx, cs)
		persistentVolumes := kubernetes.NewPersistentVolumeWatch(ctx, cs)
		persistentVolumeClaims := kubernetes.NewPersistentVolumeClaimWatch(ctx, cs)
		runtimeObjectStoreImpl := &kubernetes.RuntimeObjectStoreImpl{
			StatefulSetsStore:          statefulSets,
			PodsStore:                  pods,
			PersistentVolumeStore:      persistentVolumes,
			PersistentVolumeClaimStore: persistentVolumeClaims,
		}

		// Sanitize user input
		sort.Strings(options.conditions)

		// Eviction Filtering
		pf := []kubernetes.PodFilterFunc{kubernetes.MirrorPodFilter}
		if !options.evictLocalStoragePods {
			pf = append(pf, kubernetes.LocalStoragePodFilter)
		}

		apiResources, err := kubernetes.GetAPIResourcesForGVK(cs, options.doNotEvictPodControlledBy, log)
		if err != nil {
			return fmt.Errorf("failed to get resources for controlby filtering for eviction: %v", err)
		}
		if len(apiResources) > 0 {
			for _, apiResource := range apiResources {
				if apiResource == nil {
					log.Info("Filtering pod that are uncontrolled for eviction")
				} else {
					log.Info("Filtering pods controlled by apiresource for eviction", zap.Any("apiresource", *apiResource))
				}
			}
			pf = append(pf, kubernetes.NewPodControlledByFilter(apiResources))
		}
		systemKnownAnnotations := []string{
			// https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-types-of-pods-can-prevent-ca-from-removing-a-node
			"cluster-autoscaler.kubernetes.io/safe-to-evict=false",
		}
		pf = append(pf, kubernetes.UnprotectedPodFilter(runtimeObjectStoreImpl, false, append(systemKnownAnnotations, options.protectedPodAnnotations...)...))

		// Cordon Filtering
		podFilterCordon := []kubernetes.PodFilterFunc{}
		if !options.cordonLocalStoragePods {
			podFilterCordon = append(podFilterCordon, kubernetes.LocalStoragePodFilter)
		}
		apiResourcesCordon, err := kubernetes.GetAPIResourcesForGVK(cs, options.doNotCordonPodControlledBy, log)
		if err != nil {
			return fmt.Errorf("failed to get resources for 'controlledBy' filtering for cordon: %v", err)
		}
		if len(apiResourcesCordon) > 0 {
			for _, apiResource := range apiResourcesCordon {
				if apiResource == nil {
					log.Info("Filtering pods that are uncontrolled for cordon")
				} else {
					log.Info("Filtering pods controlled by apiresource for cordon", zap.Any("apiresource", *apiResource))
				}
			}
			podFilterCordon = append(podFilterCordon, kubernetes.NewPodControlledByFilter(apiResourcesCordon))
		}
		podFilterCordon = append(podFilterCordon, kubernetes.UnprotectedPodFilter(runtimeObjectStoreImpl, true, options.cordonProtectedPodAnnotations...))

		// Cordon limiter
		cordonLimiter := kubernetes.NewCordonLimiter(log)
		cordonLimiter.SetSkipLimiterSelector(options.skipCordonLimiterNodeAnnotationSelector)
		for p, f := range options.maxSimultaneousCordonFunctions {
			cordonLimiter.AddLimiter("MaxSimultaneousCordon:"+p, f)
		}
		for p, f := range options.maxSimultaneousCordonForLabelsFunctions {
			cordonLimiter.AddLimiter("MaxSimultaneousCordonLimiterForLabels:"+p, f)
		}
		for p, f := range options.maxSimultaneousCordonForTaintsFunctions {
			cordonLimiter.AddLimiter("MaxSimultaneousCordonLimiterForTaints:"+p, f)
		}
		globalLocker := kubernetes.NewGlobalBlocker(log)
		for p, f := range options.maxNotReadyNodesFunctions {
			globalLocker.AddBlocker("MaxNotReadyNodes:"+p, f(runtimeObjectStoreImpl, log), options.maxNotReadyNodesPeriod)
		}
		for p, f := range options.maxPendingPodsFunctions {
			globalLocker.AddBlocker("MaxPendingPods:"+p, f(runtimeObjectStoreImpl, log), options.maxPendingPodsPeriod)
		}

		for name, blockStateFunc := range globalLocker.GetBlockStateCacheAccessor() {
			localFunc := blockStateFunc
			cordonLimiter.AddLimiter(name, func(_ *core.Node, _, _ []*core.Node) (bool, error) { return !localFunc(), nil })
		}

		nodeReplacementLimiter := kubernetes.NewNodeReplacementLimiter(options.maxNodeReplacementPerHour, time.Now())

		b := record.NewBroadcaster()
		b.StartRecordingToSink(&typedcore.EventSinkImpl{Interface: typedcore.New(cs.CoreV1().RESTClient()).Events("")})
		k8sEventRecorder := b.NewRecorder(scheme.Scheme, core.EventSource{Component: kubernetes.Component})
		eventRecorder := kubernetes.NewEventRecorder(k8sEventRecorder)

		consolidatedOptInAnnotations := append(options.optInPodAnnotations, options.shortLivedPodAnnotations...)

		globalConfig := kubernetes.GlobalConfig{
			Context:                            ctx,
			ConfigName:                         options.configName,
			SuppliedConditions:                 options.suppliedConditions,
			PVCManagementEnableIfNoEvictionUrl: options.pvcManagementByDefault,
		}

		drainerSkipPodFilter := kubernetes.NewPodFiltersIgnoreCompletedPods(
			kubernetes.NewPodFiltersIgnoreShortLivedPods(
				kubernetes.NewPodFiltersWithOptInFirst(kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, consolidatedOptInAnnotations...), kubernetes.NewPodFilters(pf...)),
				runtimeObjectStoreImpl, options.shortLivedPodAnnotations...))

		cordonDrainer := kubernetes.NewAPICordonDrainer(cs,
			eventRecorder,
			kubernetes.MaxGracePeriod(options.minEvictionTimeout),
			kubernetes.EvictionHeadroom(options.evictionHeadroom),
			kubernetes.WithSkipDrain(options.skipDrain),
			kubernetes.WithPodFilter(drainerSkipPodFilter),
			kubernetes.WithCordonLimiter(cordonLimiter),
			kubernetes.WithNodeReplacementLimiter(nodeReplacementLimiter),
			kubernetes.WithStorageClassesAllowingDeletion(options.storageClassesAllowingVolumeDeletion),
			kubernetes.WithMaxDrainAttemptsBeforeFail(options.maxDrainAttemptsBeforeFail),
			kubernetes.WithGlobalConfig(globalConfig),
			kubernetes.WithAPICordonDrainerLogger(log),
		)

		// TODO do analysis and check if   drainerSkipPodFilter = NewPodFiltersIgnoreShortLivedPods(cordonPodFilteringFunc) ?
		cordonPodFilteringFunc := kubernetes.NewPodFiltersIgnoreCompletedPods(
			kubernetes.NewPodFiltersWithOptInFirst(
				kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, consolidatedOptInAnnotations...), kubernetes.NewPodFilters(podFilterCordon...)))

		var h cache.ResourceEventHandler = kubernetes.NewDrainingResourceEventHandler(
			cs,
			cordonDrainer,
			runtimeObjectStoreImpl,
			eventRecorder,
			kubernetes.WithLogger(log),
			kubernetes.WithDrainBuffer(options.drainBuffer),
			kubernetes.WithSchedulingBackoffDelay(options.schedulingRetryBackoffDelay),
			kubernetes.WithDurationWithCompletedStatusBeforeReplacement(options.durationBeforeReplacement),
			kubernetes.WithDrainGroups(options.drainGroupLabelKey),
			kubernetes.WithGlobalConfigHandler(globalConfig),
			kubernetes.WithCordonPodFilter(cordonPodFilteringFunc),
			kubernetes.WithGlobalBlocking(globalLocker),
			kubernetes.WithPreprovisioningConfiguration(kubernetes.NodePreprovisioningConfiguration{Timeout: options.preprovisioningTimeout, CheckPeriod: options.preprovisioningCheckPeriod, AllNodesByDefault: options.preprovisioningActivatedByDefault}))

		if options.dryRun {
			h = cache.FilteringResourceEventHandler{
				FilterFunc: kubernetes.NewNodeProcessed().Filter,
				Handler: kubernetes.NewDrainingResourceEventHandler(
					cs,
					&kubernetes.NoopCordonDrainer{},
					runtimeObjectStoreImpl,
					eventRecorder,
					kubernetes.WithLogger(log),
					kubernetes.WithDrainBuffer(options.drainBuffer),
					kubernetes.WithSchedulingBackoffDelay(options.schedulingRetryBackoffDelay),
					kubernetes.WithDurationWithCompletedStatusBeforeReplacement(options.durationBeforeReplacement),
					kubernetes.WithDrainGroups(options.drainGroupLabelKey),
					kubernetes.WithGlobalBlocking(globalLocker),
					kubernetes.WithGlobalConfigHandler(globalConfig)),
			}
		}

		if len(options.nodeLabels) > 0 {
			log.Info("node labels", zap.Any("labels", options.nodeLabels))
			if options.nodeLabelsExpr != "" {
				return fmt.Errorf("nodeLabels and NodeLabelsExpr cannot both be set")
			}
			if ptrStr, err := kubernetes.ConvertLabelsToFilterExpr(options.nodeLabels); err != nil {
				return err
			} else {
				options.nodeLabelsExpr = *ptrStr
			}
		}

		var nodeLabelFilter cache.ResourceEventHandler
		log.Debug("label expression", zap.Any("expr", options.nodeLabelsExpr))

		nodeLabelFilterFunc, err := kubernetes.NewNodeLabelFilter(options.nodeLabelsExpr, log)
		if err != nil {
			log.Sugar().Fatalf("Failed to parse node label expression: %v", err)
		}

		if options.noLegacyNodeHandler {
			nodeLabelFilter = cache.FilteringResourceEventHandler{FilterFunc: func(obj interface{}) bool {
				return false
			}, Handler: nil}
		} else {
			nodeLabelFilter = cache.FilteringResourceEventHandler{FilterFunc: nodeLabelFilterFunc, Handler: h}
		}
		nodes := kubernetes.NewNodeWatch(ctx, cs, nodeLabelFilter)
		runtimeObjectStoreImpl.NodesStore = nodes
		// storeCloserFunc := runtimeObjectStoreImpl.Run(log)
		// defer storeCloserFunc()
		cordonLimiter.SetNodeLister(nodes)
		cordonDrainer.SetRuntimeObjectStore(runtimeObjectStoreImpl)

		id, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname: %v", err)
		}

		scopeObserver := kubernetes.NewScopeObserver(cs, globalConfig, runtimeObjectStoreImpl, options.scopeAnalysisPeriod, cordonPodFilteringFunc,
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, options.optInPodAnnotations...),
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(runtimeObjectStoreImpl, options.cordonProtectedPodAnnotations...),
			nodeLabelFilterFunc, log)
		go scopeObserver.Run(ctx.Done())
		if options.resetScopeLabel == true {
			go scopeObserver.Reset()
		}

		lock, err := resourcelock.New(
			resourcelock.EndpointsLeasesResourceLock,
			options.namespace,
			options.leaderElectionTokenName,
			cs.CoreV1(),
			cs.CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity:      id,
				EventRecorder: k8sEventRecorder,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create lock: %v", err)
		}

		filters := filtersDefinitions{
			cordonPodFilter: cordonPodFilteringFunc,
			drainPodFilter:  drainerSkipPodFilter,
			nodeLabelFilter: nodeLabelFilterFunc,
		}
		if err = controllerRuntimeBootstrap(options, cfg, cordonDrainer, filters, runtimeObjectStoreImpl, globalConfig, log); err != nil {
			return fmt.Errorf("failed to bootstrap the controller runtime section: %v", err)
		}

		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: options.leaderElectionLeaseDuration,
			RenewDeadline: options.leaderElectionRenewDeadline,
			RetryPeriod:   options.leaderElectionRetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					log.Info("watchers are running")
					if errLE := kubernetes.Await(nodes, pods, statefulSets, persistentVolumes, persistentVolumeClaims, globalLocker); errLE != nil {
						panic("leader election, error watching: " + errLE.Error())
					}

				},
				OnStoppedLeading: func() {
					panic("lost leader election")
				},
			},
		})
		return nil
	}

	err := root.Execute()
	_ = zap.L().Sync()
	if err != nil {
		zap.L().Fatal("Program exit on error", zap.Error(err))
	}

}

func GetKubernetesClientSet(config *kubeclient.Config) (*client.Clientset, error) {
	if err := k8sclient.DecorateWithRateLimiter(config, "default"); err != nil {
		return nil, err
	}
	c, err := kubeclient.NewKubeConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed create Kubernetes client configuration: %v", err)
	}

	cs, err := client.NewForConfig(c)
	if err != nil {
		return nil, fmt.Errorf("failed create Kubernetes client: %v", err)
	}
	return cs, nil
}

type httpRunner struct {
	address string
	logger  *zap.Logger
	h       map[string]http.Handler
}

func (r *httpRunner) Run(stop <-chan struct{}) {
	rt := httprouter.New()
	for path, handler := range r.h {
		rt.Handler("GET", path, handler)
	}

	s := &http.Server{Addr: r.address, Handler: rt}
	ctx, cancel := context.WithTimeout(context.Background(), 0*time.Second)
	go func() {
		<-stop
		if err := s.Shutdown(ctx); err != nil {
			r.logger.Error("Failed to shutdown httpRunner", zap.Error(err))
			return
		}
	}()
	if err := s.ListenAndServe(); err != nil {
		r.logger.Error("Failed to ListenAndServe httpRunner", zap.Error(err))
	}
	cancel()
}

// TODO should we put this in globalConfig ?
type filtersDefinitions struct {
	cordonPodFilter kubernetes.PodFilterFunc
	drainPodFilter  kubernetes.PodFilterFunc

	nodeLabelFilter kubernetes.NodeLabelFilterFunc
}

// controllerRuntimeBootstrap This function is not called, it is just there to prepare the ground in terms of dependencies for next step where we will include ControllerRuntime library
func controllerRuntimeBootstrap(options *Options, cfg *controllerruntime.Config, drainer kubernetes.Drainer, filters filtersDefinitions, store kubernetes.RuntimeObjectStore, globalConfig kubernetes.GlobalConfig, zlog *zap.Logger) error {

	cfg.InfraParam.UpdateWithKubeContext(cfg.KubeClientConfig.ConfigFile, "")
	validationOptions := infraparameters.GetValidateAll()
	validationOptions.Datacenter, validationOptions.CloudProvider, validationOptions.CloudProviderProject = false, false, false
	if err := cfg.InfraParam.Validate(validationOptions); err != nil {
		return fmt.Errorf("infra param validation error: %v\n", err)
	}

	cfg.ManagerOptions.Scheme = runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		return fmt.Errorf("error while adding client-go scheme: %v\n", err)
	}
	if err := core.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		return fmt.Errorf("error while adding v1 scheme: %v\n", err)
	}

	if err := policyv1beta1.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		return fmt.Errorf("error while adding policyv1 scheme: %v\n", err)
	}

	if err := policyv1.AddToScheme(cfg.ManagerOptions.Scheme); err != nil {
		return fmt.Errorf("error while adding policyv1 scheme: %v\n", err)
	}

	mgr, logger, _, err := controllerruntime.NewManager(cfg)
	if err != nil {
		return fmt.Errorf("error while creating manager: %v\n", err)
	}

	indexer, err := index.New(mgr.GetClient(), mgr.GetCache(), logger)
	if err != nil {
		return fmt.Errorf("error while initializing informer: %v\n", err)
	}

	staticRetryStrategy := &drain.StaticRetryStrategy{
		AlertThreashold: 7,
		Delay:           options.schedulingRetryBackoffDelay,
	}
	retryWall, errRW := drain.NewRetryWall(mgr.GetClient(), mgr.GetLogger(), staticRetryStrategy)
	if errRW != nil {
		return errRW
	}

	ctx := context.Background()

	cs, err := GetKubernetesClientSet(&cfg.KubeClientConfig)
	if err != nil {
		return err
	}

	b := record.NewBroadcaster()
	b.StartRecordingToSink(&typedcore.EventSinkImpl{Interface: typedcore.New(cs.CoreV1().RESTClient()).Events("")})
	k8sEventRecorder := b.NewRecorder(scheme.Scheme, core.EventSource{Component: kubernetes.Component})
	eventRecorder := kubernetes.NewEventRecorder(k8sEventRecorder)

	pvProtector := protector.NewPVCProtector(store, zlog, globalConfig.PVCManagementEnableIfNoEvictionUrl)

	drainRunnerFactory, err := drain_runner.NewFactory(
		drain_runner.WithKubeClient(mgr.GetClient()),
		drain_runner.WithClock(&clock.RealClock{}),
		drain_runner.WithDrainer(drainer),
		drain_runner.WithPreprocessors(), // TODO when we will add the pre-provisioning pre-processor
		drain_runner.WithRerun(options.groupRunnerPeriod),
		drain_runner.WithRetryWall(retryWall),
		drain_runner.WithLogger(mgr.GetLogger()),
		drain_runner.WithSharedIndexInformer(indexer),
		drain_runner.WithPVProtector(pvProtector),
	)
	if err != nil {
		return err
	}

	drainCandidateRunnerFactory, err := candidate_runner.NewFactory(
		candidate_runner.WithDryRun(true), // TODO of course we want to remove that when we are ready
		candidate_runner.WithKubeClient(mgr.GetClient()),
		candidate_runner.WithClock(&clock.RealClock{}),
		candidate_runner.WithRerun(options.groupRunnerPeriod),
		candidate_runner.WithRetryWall(retryWall),
		candidate_runner.WithLogger(mgr.GetLogger()),
		candidate_runner.WithSharedIndexInformer(indexer),
		candidate_runner.WithCordonPodFilter(filters.cordonPodFilter),
		candidate_runner.WithEventRecorder(eventRecorder),
		candidate_runner.WithRuntimeObjectStore(store),
		candidate_runner.WithNodeLabelsFilterFunction(filters.nodeLabelFilter),
		candidate_runner.WithGlobalConfig(globalConfig),
		candidate_runner.WithMaxSimultaneousCandidates(1), // TODO should we move that to something that can be customized per user
		candidate_runner.WithDrainSimulator(drain.NewDrainSimulator(context.Background(), mgr.GetClient(), indexer, filters.drainPodFilter)),
		candidate_runner.WithNodeSorters(candidate_runner.NodeSorters{}),
		candidate_runner.WithPVProtector(pvProtector),
	)
	if err != nil {
		return err
	}

	keyGetter := groups.NewGroupKeyFromNodeMetadata(strings.Split(options.drainGroupLabelKey, ","), []string{kubernetes.DrainGroupAnnotation}, kubernetes.DrainGroupOverrideAnnotation)

	groupRegistry := groups.NewGroupRegistry(ctx, mgr.GetClient(), mgr.GetLogger(), eventRecorder, keyGetter, drainRunnerFactory, drainCandidateRunnerFactory, filters.nodeLabelFilter, store.HasSynced)
	if err = groupRegistry.SetupWithManager(mgr); err != nil {
		logger.Error(err, "failed to setup groupRegistry")
		return err
	}

	logger.Info("ControllerRuntime bootstrap done, running the manager")
	// Starting Manager
	go func() {
		logger.Info("Starting manager")
		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			logger.Error(err, "Controller Manager did exit with error")
			panic("Manager finished with error: " + err.Error()) // TODO remove this that is purely for testing and identifying an early exit of the code
		}
		logger.Info("Manager finished without error")
		panic("Manager finished normally") // TODO remove this that is purely for testing and identifying an early exit of the code
	}()

	return nil
}
