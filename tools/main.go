package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/DataDog/compute-go/controllerruntime"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	// Read application flags
	cfg, fs := controllerruntime.ConfigFromFlags(false, false)
	mgr, logger, _, err := controllerruntime.NewManager(cfg)
	if err != nil {
		fmt.Errorf("error while creating manager: %v\n", err)
		os.Exit(1)
	}

	root := &cobra.Command{
		Short:        "draino-tool",
		Long:         "draino-tool",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(TaintCmd(mgr))

	go mgr.Start(context.Background())
	time.Sleep(time.Second)

	if err := root.Execute(); err != nil {
		logger.Error(err, "root command exit with error")
	}

	_ = zap.L().Sync()
	if err != nil {
		zap.L().Fatal("Program exit on error", zap.Error(err))
	}
}
