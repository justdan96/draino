package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr/funcr"
	"k8s.io/client-go/util/flowcontrol"

	circuitbreaker "github.com/planetlabs/draino/internal/circuit_breaker"
)

// This test make the assumption that the file /etc/config/datadog-client.yaml is present on the system and valid for the context of the test

func main() {
	var logger = funcr.New(func(prefix, args string) {
		fmt.Println(prefix, args)
	}, funcr.Options{})

	cb, err := circuitbreaker.NewMonitorBasedCircuitBreaker("test", logger, 30*time.Second, "draino_circuit_breaker", "snowver", flowcontrol.NewFakeNeverRateLimiter(), circuitbreaker.Open)
	if err != nil {
		logger.Error(err, "can't build circuit breaker")
		os.Exit(1)
	}
	cb.Start(context.Background())
}
