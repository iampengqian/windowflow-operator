// Copyright 2026 WindowFlow contributors. SPDX-License-Identifier: Apache-2.0
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	api "github.com/iampengqian/windowflow-operator/api/v1alpha1"
	"github.com/iampengqian/windowflow-operator/internal/controller"
	"github.com/iampengqian/windowflow-operator/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: windowflow manager|stage|clean")
	}
	switch os.Args[1] {
	case "stage", "clean":
		var cfg worker.Config
		decoder := json.NewDecoder(strings.NewReader(os.Getenv("WINDOWFLOW_WORKER_CONFIG")))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg); err != nil {
			return fmt.Errorf("invalid WINDOWFLOW_WORKER_CONFIG: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			return fmt.Errorf("worker config must contain exactly one JSON object")
		}
		if cfg.Action != os.Args[1] {
			return fmt.Errorf("worker action does not match subcommand")
		}
		return worker.Run(ctrl.SetupSignalHandler(), cfg, envOr("WINDOWFLOW_CACHE_ROOT", "/cache"), envOr("WINDOWFLOW_SOURCE_ROOT", "/source"))
	case "manager":
		flags := flag.NewFlagSet("manager", flag.ContinueOnError)
		leader := flags.Bool("leader-elect", true, "enable Kubernetes leader election")
		leaderNamespace := flags.String("leader-election-namespace", "windowflow-system", "namespace for manager leader election")
		metrics := flags.String("metrics-bind-address", "0", "metrics listener (disabled by default)")
		probes := flags.String("health-probe-bind-address", ":8081", "health probe listener")
		if err := flags.Parse(os.Args[2:]); err != nil {
			return err
		}
		ctrl.SetLogger(zap.New())
		scheme := runtime.NewScheme()
		for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, coordv1.AddToScheme, api.AddToScheme} {
			if err := add(scheme); err != nil {
				return err
			}
		}
		config, err := ctrl.GetConfig()
		if err != nil {
			return err
		}
		mgr, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme, LeaderElection: *leader, LeaderElectionNamespace: *leaderNamespace, LeaderElectionID: "windowflow-manager", Metrics: metricsserver.Options{BindAddress: *metrics}, HealthProbeBindAddress: *probes})
		if err != nil {
			return err
		}
		if err := (&controller.WindowPlanReconciler{Client: mgr.GetClient(), Scheme: scheme}).SetupWithManager(mgr); err != nil {
			return err
		}
		if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
			return err
		}
		if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
			return err
		}
		return mgr.Start(ctrl.SetupSignalHandler())
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
