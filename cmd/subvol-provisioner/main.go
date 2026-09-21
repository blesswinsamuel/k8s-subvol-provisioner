package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/apis/config"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/controller"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver/btrfs"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver/dir"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver/mock"
	"github.com/blesswinsamuel/k8s-subvol-provisioner/pkg/driver/zfs"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type runFlags struct {
	kubeconfig      string
	nodeName        string
	provisionerName string
	hostPrefix      string
	resyncPeriod    time.Duration
	logLevel        string
	enableMock      bool
}

func main() {
	flags := &runFlags{}

	rootCmd := &cobra.Command{
		Use:   "subvol-provisioner",
		Short: "k8s-subvol-provisioner dynamically provisions native ZFS datasets and Btrfs subvolumes for Kubernetes",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(flags)
		},
	}

	rootCmd.Flags().StringVar(&flags.kubeconfig, "kubeconfig", "", "Path to kubeconfig file (defaults to in-cluster config)")
	rootCmd.Flags().StringVar(&flags.nodeName, "node-name", os.Getenv("NODE_NAME"), "Node name this provisioner agent is running on (defaults to NODE_NAME env var)")
	rootCmd.Flags().StringVar(&flags.provisionerName, "provisioner-name", getEnvOrDefault("PROVISIONER_NAME", config.DefaultProvisionerName), "Provisioner name handled by this instance")
	rootCmd.Flags().StringVar(&flags.hostPrefix, "host-prefix", os.Getenv("HOST_PREFIX"), "Host filesystem prefix path when running in a container (defaults to HOST_PREFIX env var)")
	rootCmd.Flags().DurationVar(&flags.resyncPeriod, "resync-period", 15*time.Second, "Informer resync period")
	rootCmd.Flags().StringVar(&flags.logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.Flags().BoolVar(&flags.enableMock, "enable-mock", false, "Enable in-memory mock driver for development")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version of subvol-provisioner",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("k8s-subvol-provisioner %s (commit: %s, built at: %s)\n", version, commit, date)
		},
	}
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(flags *runFlags) error {
	level, err := zerolog.ParseLevel(flags.logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	log.Info().
		Str("version", version).
		Str("commit", commit).
		Str("nodeName", flags.nodeName).
		Str("provisioner", flags.provisionerName).
		Msg("Initializing k8s-subvol-provisioner")

	// Setup Kubernetes client
	restConfig, err := buildKubeConfig(flags.kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize kubernetes client: %w", err)
	}

	// Setup event recorder
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{
		Component: flags.provisionerName,
		Host:      flags.nodeName,
	})

	// Setup Drivers
	drivers := map[string]driver.Driver{
		"zfs":   zfs.New(),
		"btrfs": btrfs.New(),
		"dir":   dir.New(),
	}
	if flags.enableMock {
		drivers["mock"] = mock.New()
	}

	ctrl, err := controller.NewController(controller.ControllerOptions{
		Client:          client,
		NodeName:        flags.nodeName,
		ProvisionerName: flags.provisionerName,
		HostPrefix:      flags.hostPrefix,
		Drivers:         drivers,
		Recorder:        recorder,
		ResyncPeriod:    flags.resyncPeriod,
	})
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return ctrl.Run(ctx)
}

func buildKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
}

func getEnvOrDefault(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}
