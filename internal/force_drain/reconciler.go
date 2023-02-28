package forcedrain

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/drain_runner"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cr "sigs.k8s.io/controller-runtime"
)

type ForceDrainController struct {
	kclient           client.Client
	drainer           kubernetes.Drainer
	conditions        []kubernetes.SuppliedCondition
	clock             clock.Clock
	logger            logr.Logger
	nodeFilteringFunc kubernetes.NodeLabelFilterFunc
	hasSyncedFunc     HasSyncedFunc
}

type HasSyncedFunc func() bool

func NewPriorityDeletionController(kclient client.Client, drainer kubernetes.Drainer, conditions []kubernetes.SuppliedCondition, logger logr.Logger, syncFunc HasSyncedFunc, nodeFilteringFunc kubernetes.NodeLabelFilterFunc) *ForceDrainController {
	return &ForceDrainController{
		kclient:           kclient,
		drainer:           drainer,
		conditions:        conditions,
		clock:             clock.RealClock{},
		nodeFilteringFunc: nodeFilteringFunc,
		logger:            logger.WithName("ForceDrainController"),
		hasSyncedFunc:     syncFunc,
	}
}

// Reconcile register the node in the reverse index per ProviderIP
func (ctrl *ForceDrainController) Reconcile(ctx context.Context, req cr.Request) (cr.Result, error) {
	if !ctrl.hasSyncedFunc() {
		return cr.Result{RequeueAfter: 5 * time.Second}, nil
	}

	var node corev1.Node
	if err := ctrl.kclient.Get(ctx, req.NamespacedName, &node); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return cr.Result{}, fmt.Errorf("get Node Fails: %v", err)
	}

	// ignore all nodes that are not in scope
	if ctrl.nodeFilteringFunc == nil || ctrl.nodeFilteringFunc(node) {
		return cr.Result{}, nil
	}

	// The node should have at least one force drain condition
	badConditions := kubernetes.GetNodeOffendingConditions(&node, ctrl.conditions)
	if len(badConditions) == 0 {
		return cr.Result{}, nil
	}
	if !kubernetes.AtLeastOneForceDrainCondition(badConditions) {
		return cr.Result{}, nil
	}

	// If the taint exists already, it means that another part of the code is already working on this node.
	// So we'll either expect it to remove the taint or finish the processing.
	if _, exist := k8sclient.GetNLATaint(&node); exist {
		return cr.Result{}, nil
	}

	ctrl.logger.Info("found node with force drain condition", "node", node.Name)

	// Adding an NLA taint might fail in case of an outdated node object
	freshNode, err := k8sclient.AddNLATaint(ctx, ctrl.kclient, &node, ctrl.clock.Now(), k8sclient.TaintForceDraining)
	if err != nil {
		return cr.Result{}, err
	}

	err = ctrl.drainer.ForceDrain(ctx, freshNode)
	if err != nil {
		failureCause := kubernetes.GetFailureCause(err)
		drain_runner.CounterDrainedNodes(&node, drain_runner.DrainedNodeResultFailed, badConditions, failureCause, true)
		// Removing an NLA taint might fail in case of an outdated node object
		_, _ = k8sclient.RemoveNLATaint(ctx, ctrl.kclient, freshNode)
		return cr.Result{}, err
	}

	drain_runner.CounterDrainedNodes(&node, drain_runner.DrainedNodeResultSucceeded, badConditions, "", true)
	ctrl.logger.Info("sucessfully force-drained node", "node", node.Name)
	// Adding an NLA taint might fail in case of an outdated node object
	_, _ = k8sclient.AddNLATaint(ctx, ctrl.kclient, freshNode, ctrl.clock.Now(), k8sclient.TaintForceDrained)
	return cr.Result{}, nil
}

// SetupWithManager setups the controller with goroutine and predicates
func (ctrl *ForceDrainController) SetupWithManager(mgr cr.Manager) error {
	return cr.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		For(&corev1.Node{}).
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
		Complete(ctrl)
}
