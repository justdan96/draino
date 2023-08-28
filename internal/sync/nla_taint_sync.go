package sync

import (
	"context"
	"fmt"
	"strings"

	"github.com/DataDog/compute-go/logs"
	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NLATaintSynchronizer interface {
	// SyncNodeTaintToPods will sync the NLA taint to all pods of the given node
	SyncNodeTaintToPods(context.Context, *corev1.Node) error
}

type taintSynchronizerImpl struct {
	kubeClient    client.Client
	logger        logr.Logger
	clock         clock.Clock
	podIndexer    index.PodIndexer
	podFilterFunc kubernetes.PodFilterFunc
}

func NewNLATaintSynchronizer(kubeClient client.Client, logger logr.Logger, clock clock.Clock, podIndexer index.PodIndexer, podFilterFunc kubernetes.PodFilterFunc) NLATaintSynchronizer {
	return &taintSynchronizerImpl{
		kubeClient:    kubeClient,
		logger:        logger.WithName("NLATaintSynchronizaer"),
		clock:         clock,
		podIndexer:    podIndexer,
		podFilterFunc: podFilterFunc,
	}
}

func (t *taintSynchronizerImpl) SyncNodeTaintToPods(ctx context.Context, node *corev1.Node) error {
	_, taintExists := k8sclient.GetNLATaint(node)
	t.logger.V(logs.ZapDebug).Info("received node with taint", "node", node.Name, "taint_exists", taintExists)

	pods, err := t.podIndexer.GetPodsByNode(ctx, node.Name)
	if err != nil {
		return err
	}

	var compoundErrors []string
	t.logger.V(logs.ZapDebug).Info("found ... pods to sync", "node", node.Name, "pod_count", pods)

	for _, pod := range pods {
		pass, _, err := t.podFilterFunc(*pod)
		if err != nil {
			t.logger.Error(err, "failed to filter pod", "node", node.Name, "pod", pod.Name, "pod_namespace", pod.Namespace, "should_have_condition", taintExists)
			compoundErrors = append(compoundErrors, err.Error())
			continue
		}
		if !pass {
			continue
		}
		if err := t.syncPodCondition(ctx, pod, taintExists); err != nil {
			t.logger.Error(err, "failed to sync pod condition", "node", node.Name, "pod", pod.Name, "pod_namespace", pod.Namespace, "should_have_condition", taintExists)
			compoundErrors = append(compoundErrors, err.Error())
		}
	}

	if len(compoundErrors) > 0 {
		return fmt.Errorf("failed to sync taint to pods: %s", strings.Join(compoundErrors, ";"))
	}
	return nil
}

func (t *taintSynchronizerImpl) syncPodCondition(ctx context.Context, pod *corev1.Pod, shouldHaveCondition bool) error {
	_, exists := k8sclient.GetPodNLACondition(pod)
	logger := t.logger.WithValues("node", pod.Spec.NodeName, "pod", pod.Name, "pod_namespace", pod.Namespace, "should_have_condition", shouldHaveCondition, "has_condition", exists)

	logger.V(logs.ZapDebug).Info("received pod with condition")
	if exists == shouldHaveCondition {
		logger.V(logs.ZapDebug).Info("expected condition is matching NLA taint")
		return nil
	}

	var err error
	if shouldHaveCondition {
		logger.V(logs.ZapDebug).Info("adding condition to pod")
		_, err = k8sclient.SetOrUpdatePodNLACondition(ctx, t.kubeClient, pod, t.clock.Now())
	} else {
		logger.V(logs.ZapDebug).Info("removing condition from pod")
		_, _, err = k8sclient.RemovePodNLACondition(ctx, t.kubeClient, pod)
	}

	return err
}
