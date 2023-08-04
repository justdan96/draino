all: build

# Build manager binary
build: fmt vet
        CGO_ENABLED=0 go build -o bin/draino ./cmd/draino/*.go

# Run unittest
test: fmt vet
	go test ./...

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

run:
	go run cmd/draino/*.go \
				--kube-client-qps=20 \
        --kube-client-burst=150 \
        --namespace=cluster-controllers \
  --leader-elect-id=draino-standard \
  --leader-resource-lock=configmapsleases \
  --leader-elect-lease=15s \
  --leader-elect-renew=10s \
  --leader-elect-namespace=cluster-controllers \
        --max-notready-nodes=10% --max-notready-nodes=50 \
        --max-notready-nodes-period=60s \
        --max-pending-pods=10% \
        --max-pending-pods-period=60s \
        --scope-analysis-period=60s \
        --retry-backoff-delay=23m \
        --max-drain-attempts-before-fail=7 \
        --drain-buffer=3m \
        --drain-group-labels=nodegroups.datadoghq.com/namespace \
        --evict-emptydir-pods=true \
        --eviction-headroom=10s \
        --min-eviction-timeout=10m0s \
        --node-label-expr="(metadata.labels['nodegroups.datadoghq.com/cluster-autoscaler'] matches 'true' && not (metadata.labels['node-lifecycle.datadoghq.com/enabled'] matches 'false')) || (metadata.labels['node-lifecycle.datadoghq.com/enabled'] matches 'true')" \
        --protected-pod-annotation=cluster-autoscaler.kubernetes.io/daemonset-pod=true \
        --protected-pod-annotation=node-lifecycle.datadoghq.com/cordon-node-but-do-not-evict-pod=true \
        --protected-pod-annotation=node-lifecycle.datadoghq.com/enabled=false \
        --do-not-evict-pod-controlled-by=DaemonSet \
        --do-not-evict-pod-controlled-by=ExtendedDaemonSetReplicaSet \
        --cordon-protected-pod-annotation=node-lifecycle.datadoghq.com/enabled=false \
        --do-not-cordon-pod-controlled-by= \
        --short-lived-pod-annotation=node-lifecycle.datadoghq.com/short-lived-pod=true \
        --opt-in-pod-annotation=node-lifecycle.datadoghq.com/enabled=true \
        --pvc-management-by-default \
        --config-name=standard \
        --node-conditions=nodegroup-update \
        --node-conditions=LifetimeExceeded \
        --node-conditions=ec2-host-retirement \
        --node-conditions=containerd-task-blocked \
        --node-conditions=shim-blocked \
        --node-conditions=containerd-issue \
        --node-conditions=containerd-update \
        --node-conditions=hung-task \
        --node-conditions='disk-issue={"priority":3000,"expectedResolutionTime":"30m","rateLimitQPS":0.02,"forceDrain":true}' \
        --node-conditions=network-issue \
        --node-conditions=memory-fault \
        --node-conditions=kernel-issue \
        --node-conditions=kernel-update \
        --node-conditions=kubelet-issue \
        --node-conditions=kubelet-update \
        --node-conditions=cilium-issue \
        --node-conditions=cilium-update \
        --node-conditions=compliance-update \
        --node-conditions=performance \
        --node-conditions=application \
        --node-conditions=esr \
        --node-conditions=gameday \
        --node-conditions=nodeless-rotation \
        --node-conditions=maintenance \
        --node-conditions=incident \
        --node-conditions='other={"priority":1000}' \
        --node-conditions='cloud-object-change' \
        --no-legacy-node-handler=true \
        --node-conditions='cluster-upgrade={"delay":"6h"}' \
        --kube-cluster-name=chillpenguin
