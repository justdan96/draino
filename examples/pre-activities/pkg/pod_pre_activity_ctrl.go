package pkg

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/cache"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = &PodPreActivityController{}

// PodPreActivityController will handle all the pod events (CRUD) and perform actions if needed.
// The flow is based on the pre-activity contract whereas the action is just a sleep right now.
// https://datadoghq.atlassian.net/wiki/spaces/K8S/pages/2791706268/NLA+Pre-activies+API+proposal
type PodPreActivityController struct {
	kclient client.Client
	logger  logr.Logger
	cache   cache.ThreadSafeStore
}

func NewPodPreActivityController(kclient client.Client, logger logr.Logger) *PodPreActivityController {
	return &PodPreActivityController{
		kclient: kclient,
		logger:  logger.WithName("PodPreActivityController"),
		cache:   cache.NewThreadSafeStore(nil, nil),
	}
}

// Reconcile will receive a request whenever there was a pod update (CRUD).
// This means that it will also be executed when the controller is updating the annotation.
func (podCtrl *PodPreActivityController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var pod corev1.Pod
	err := podCtrl.kclient.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, &pod)
	if err != nil {
		// If pod was not found, it means that it was deleted already.
		// In this case we don't want to take any actions, so we'll just drop the message.
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	// The following part is handling the pre requisites for the contract
	// Which basically means, that the pod has to have the pre-activity annotation attached & includes the NLA condition.
	preActivityVal, found := pod.Annotations[PreActivityAnnotation]
	if !found {
		return reconcile.Result{}, nil
	}
	if hasCondition := HasPodNLACondition(&pod); !hasCondition {
		return reconcile.Result{}, nil
	}

	logger := podCtrl.logger.WithValues("pod", pod.Name, "pod_namespace", pod.Namespace, "value", preActivityVal)

	logger.Info("process pre-activity")
	_, preActivityRunning := podCtrl.cache.Get(createPodCacheKey(&pod))
	switch preActivityVal {
	case string(PreActivityStateWaiting):
		if !preActivityRunning { // Pre-activity should be executed, but processing didn't start yet.
			go podCtrl.processPreActivity(ctx, logger, &pod)
		} else { // Pre-activity was reset by NLA, so we should stop the processing
			// We don't update the pre-activity value, because it was already done by NLA
			podCtrl.cancelPreActivity(&pod)
		}
		return reconcile.Result{}, nil
	case string(PreActivityStateProcessing):
		// In case it's supposed to be processing, but there is no thread in the cache, we'll set the activity to failed.
		// Otherwise, we'll just ignore the update event.
		if !preActivityRunning {
			if _, _, err := SetPodPreActivityAnnotationVal(ctx, podCtrl.kclient, &pod, PreActivityStateFailed); err != nil {
				logger.Error(err, "failed to update pod pre-activity value", "new_value", PreActivityStateFailed)
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	case string(PreActivityStateDone), string(PreActivityStateFailed):
		// In this case, the pre-activity should be done
		// Just to make sure we don't leak anything in case of an error, we'll cancel the execution
		podCtrl.cancelPreActivity(&pod)
		return reconcile.Result{}, nil
	default: // Should never be reached
		podCtrl.logger.Info("invalid pre activity value", "pod", pod.Name, "pod_namespace", pod.Namespace, "value", preActivityVal)
		return reconcile.Result{}, nil
	}
}

// processPreActivity is a wrapper function that handles things like setting annotations or adding the process to the cache.
func (podCtrl *PodPreActivityController) processPreActivity(ctx context.Context, logger logr.Logger, pod *corev1.Pod) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	podCtrl.cache.Add(createPodCacheKey(pod), cancel)
	defer podCtrl.cache.Delete(createPodCacheKey(pod))

	newPod, _, err := SetPodPreActivityAnnotationVal(ctx, podCtrl.kclient, pod, PreActivityStateProcessing)
	if err != nil {
		logger.Error(err, "failed to set pod pre-activity value", "new_value", PreActivityStateDone)
		return
	}

	// TODO: Here comes your logic
	logger.Info("start code execution")
	err = executeAmazingLogic(ctx)
	if err != nil {
		logger.Error(err, "failed to execute logic")
		return
	}
	logger.Info("successfully executed code")

	_, _, err = SetPodPreActivityAnnotationVal(ctx, podCtrl.kclient, newPod, PreActivityStateDone)
	if err != nil {
		logger.Error(err, "failed to set pod pre-activity value", "new_value", PreActivityStateDone)
		return
	}
	logger.Info("successfully ran pre-activity")
}

// cancelPreActivity will terminate the pre-activity processing
func (podCtrl *PodPreActivityController) cancelPreActivity(pod *corev1.Pod) {
	activityCancelFuncInt, found := podCtrl.cache.Get(createPodCacheKey(pod))
	if !found {
		return
	}

	defer podCtrl.cache.Delete(createPodCacheKey(pod))
	if activityCancelFunc, ok := activityCancelFuncInt.(context.CancelFunc); ok {
		activityCancelFunc()
	}
}

func (podCtrl *PodPreActivityController) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(podCtrl)
}

func createPodCacheKey(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

func executeAmazingLogic(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-time.After(time.Minute):
	}

	// Will return an error in case the context was canceled
	return ctx.Err()
}
