package main

import (
	"os"
	"pre-activity-example/pkg"

	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

func init() {
	log.SetLogger(zap.New())
}

func main() {
	entryLog := log.Log.WithName("entrypoint")

	// We are using the controller runtime in this case, but this is only an abstraction over a watcher.
	// You can use what ever you want to run the operator/controller.
	// https://github.com/kubernetes-sigs/controller-runtime
	mgr, err := manager.New(config.GetConfigOrDie(), manager.Options{})
	if err != nil {
		entryLog.Error(err, "unable to set up overall controller manager")
		os.Exit(1)
	}

	podCtrl := pkg.NewPodPreActivityController(mgr.GetClient(), mgr.GetLogger())
	if err := podCtrl.SetupWithManager(mgr); err != nil {
		entryLog.Error(err, "unable to set up pod controller with manager")
		os.Exit(1)
	}

	entryLog.Info("starting manager")
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		entryLog.Error(err, "unable to run manager")
		os.Exit(1)
	}
	entryLog.Info("stopping manager")
}
