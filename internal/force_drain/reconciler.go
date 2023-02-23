package forcedrain

import (
	"context"
	"fmt"
	"time"

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
	kclient       client.Client
	prioRemover   kubernetes.Drainer
	conditions    []kubernetes.SuppliedCondition
	clock         clock.Clock
	hasSyncedFunc func() bool
}

func NewPriorityDeletionController() *ForceDrainController {
	return &ForceDrainController{}
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

	// The node should have at least one unrecoverable condition
	badConditions := kubernetes.GetNodeOffendingConditions(&node, ctrl.conditions)
	if len(badConditions) == 0 {
		return cr.Result{}, nil
	}
	if !kubernetes.AtLeastOneForceEvictCondition(badConditions) {
		return cr.Result{}, nil
	}

	freshNode, err := k8sclient.AddNLATaint(ctx, ctrl.kclient, &node, ctrl.clock.Now(), k8sclient.TaintForceDrain)
	if err != nil {
		return cr.Result{}, err
	}

	err = ctrl.prioRemover.ForceDrain(ctx, freshNode)

	return cr.Result{}, err
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
