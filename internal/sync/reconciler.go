package sync

import (
	"context"
	"strings"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/drain_runner/pre_processor"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type NodeTaintSyncReconciler struct {
	kubeClient    client.Client
	synchonizator NLATaintSynchronizer
	logger        logr.Logger
}

func NewNodeTaintSyncReconciler(kubeClient client.Client, sync NLATaintSynchronizer, logger logr.Logger) *NodeTaintSyncReconciler {
	return &NodeTaintSyncReconciler{
		kubeClient:    kubeClient,
		synchonizator: sync,
		logger:        logger.WithName("NodeTaintSyncReconciler"),
	}
}

func (n *NodeTaintSyncReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var node corev1.Node
	err := n.kubeClient.Get(ctx, types.NamespacedName{Name: req.Name}, &node)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := n.synchonizator.SyncNodeTaintToPods(ctx, &node); err != nil {
		n.logger.Error(err, "failed to sync NLA taint to pods", "node", node.Name)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func MapPodToNodeName(obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	// Pod has not been scheduled yet, so we'll wait for the next update
	if pod.Spec.NodeName == "" {
		return nil
	}

	for key := range pod.Annotations {
		// For now we only care about the pods that are running pre-activities
		if strings.HasPrefix(key, pre_processor.PreActivityAnnotationPrefix) {
			return []reconcile.Request{
				{NamespacedName: types.NamespacedName{Name: pod.Spec.NodeName}},
			}
		}
	}

	return nil
}

func (n *NodeTaintSyncReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		For(&corev1.Node{}).
		Watches(
			&source.Kind{Type: &corev1.Pod{}},
			handler.EnqueueRequestsFromMapFunc(MapPodToNodeName),
		).
		Complete(n)
}
