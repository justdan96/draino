package groups

import (
	"context"
	"fmt"
	"time"

	"github.com/planetlabs/draino/internal/kubernetes/utils"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	nodeWarmUpDelay = 30 * time.Second

	// if the groupKey validation fails, and event will be added on the node with the reason
	eventGroupOverrideMisconfiguration = "GroupOverrideBadConfiguration"

	DrainCandidateRunnerName = "drain_candidate"
	DrainRunnerName          = "drain"
)

type GroupRegistry struct {
	kclient       client.Client
	logger        logr.Logger
	eventRecorder kubernetes.EventRecorder

	nodeWarmUpDelay           time.Duration
	keyGetter                 GroupKeyGetter
	groupDrainRunner          *GroupsRunner
	groupDrainCandidateRunner *GroupsRunner

	nodeFilteringFunc kubernetes.NodeLabelFilterFunc

	hasSyncedFunc func() bool
}

type RunnerInfoGetter interface {
	GetRunnerInfo() map[GroupKey]RunnerInfo
}

func (r *GroupRegistry) GetRunnerInfo() map[GroupKey]RunnerInfo {
	m := make(map[GroupKey]RunnerInfo, len(r.groupDrainRunner.running))

	for k, v := range r.groupDrainRunner.running {
		m[k] = RunnerInfo{
			Key:  k,
			Data: utils.NewDataMap(),
		}
		m[k].Data.Inject(v.Data)
	}
	for k, v := range r.groupDrainCandidateRunner.running {
		c, ok := m[k]
		if !ok {
			c = RunnerInfo{
				Key:  k,
				Data: utils.NewDataMap(),
			}
		}
		c.Data.Inject(v.Data)
		m[k] = c
	}
	return m
}

var _ RunnerInfoGetter = &GroupRegistry{}

func NewGroupRegistry(
	ctx context.Context,
	kclient client.Client,
	logger logr.Logger,
	eventRecorder kubernetes.EventRecorder,
	keyGetter GroupKeyGetter,
	drainFactory, drainCandidateFactory RunnerFactory,
	nodeFilteringFunc kubernetes.NodeLabelFilterFunc,
	hasSyncedFunc func() bool,
	maxRandomRunnerStartDelay time.Duration,
) *GroupRegistry {
	return &GroupRegistry{
		kclient:                   kclient,
		logger:                    logger,
		nodeWarmUpDelay:           nodeWarmUpDelay,
		keyGetter:                 keyGetter,
		groupDrainRunner:          NewGroupsRunner(ctx, drainFactory, logger, DrainRunnerName, maxRandomRunnerStartDelay),
		groupDrainCandidateRunner: NewGroupsRunner(ctx, drainCandidateFactory, logger, DrainCandidateRunnerName, maxRandomRunnerStartDelay),
		eventRecorder:             eventRecorder,
		nodeFilteringFunc:         nodeFilteringFunc,
		hasSyncedFunc:             hasSyncedFunc,
	}
}

// Reconcile register the node in the reverse index per ProviderIP
func (r *GroupRegistry) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if !r.hasSyncedFunc() {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: 5 * time.Second,
		}, nil
	}
	node := &v1.Node{}
	if err := r.kclient.Get(ctx, req.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get Node Fails: %v", err)
	}

	// We have to be sure that the node has a complete dataset before attempting any group creation
	if time.Now().Sub(node.GetCreationTimestamp().Time) < r.nodeWarmUpDelay {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: r.nodeWarmUpDelay,
		}, nil
	}

	// discard all node that do not match the node filtering function
	if r.nodeFilteringFunc != nil && !r.nodeFilteringFunc(node) {
		return ctrl.Result{}, nil
	}

	if valid, reason := r.keyGetter.ValidateGroupKey(node); !valid {
		r.eventRecorder.NodeEventf(ctx, node, v1.EventTypeWarning, eventGroupOverrideMisconfiguration, reason)
		r.logger.Info(eventGroupOverrideMisconfiguration+": default applies, no group override", "node", node.Name)
	}

	groupKey, err := r.keyGetter.UpdateGroupKeyOnNode(ctx, node)
	if err != nil {
		return ctrl.Result{}, err
	}

	r.groupDrainRunner.RunForGroup(groupKey)
	r.groupDrainCandidateRunner.RunForGroup(groupKey)

	return ctrl.Result{}, nil
}

// SetupWithManager setups the controller with goroutine and predicates
func (r *GroupRegistry) SetupWithManager(mgr ctrl.Manager) error {

	InitSchedulingGroupIndexer(mgr.GetCache(), r.keyGetter)

	return ctrl.NewControllerManagedBy(mgr).WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		For(&v1.Node{}).
		WithEventFilter(
			predicate.Funcs{
				CreateFunc: func(evt event.CreateEvent) bool {
					return true
				},
				DeleteFunc:  func(event.DeleteEvent) bool { return false },
				GenericFunc: func(event.GenericEvent) bool { return false },
				UpdateFunc: func(evt event.UpdateEvent) bool {
					return true
				},
			},
		).
		Complete(r)
}
