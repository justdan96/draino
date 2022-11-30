package drainer

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

type DrainPreprozessor interface {
	GetName() string
	Process(*corev1.Node) (bool, error)
}

type WaitTimePreprocessor struct {
	waitFor time.Duration
}

func NewWaitTimePreprocessor(waitFor time.Duration) DrainPreprozessor {
	return &WaitTimePreprocessor{waitFor}
}

func (_ *WaitTimePreprocessor) GetName() string {
	return "WaitTimePreprocessor"
}

func (pre *WaitTimePreprocessor) Process(node *corev1.Node) (bool, error) {
	// TODO get time when ndoe drain was scheduled
	waitUntil := node.CreationTimestamp.Time.Add(pre.waitFor)
	return waitUntil.Before(time.Now()), nil
}
