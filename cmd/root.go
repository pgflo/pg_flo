package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pgflo/pg_flo/pkg/pgflonats"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/routing"
	"github.com/pgflo/pg_flo/pkg/rules"
	"github.com/pgflo/pg_flo/pkg/sinks"
	"github.com/pgflo/pg_flo/pkg/worker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "pg_flo",
		Short: "The easiest way to move and transform data from PostgreSQL",
	}

	replicatorCmd = &cobra.Command{
		Use:   "replicator",
		Short: "Start the replicator",
		Long:  `Start the replicator to capture changes from PostgreSQL and publish to NATS`,
		Run:   runReplicator,
	}

	workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Start the worker",
		Long:  `Start the worker to process messages from NATS, apply rules, and write to sinks`,
	}

	stdoutWorkerCmd = &cobra.Command{
		Use:   "stdout",
		Short: "Start the worker with stdout sink",
		Run:   runWorker,
	}

	fileWorkerCmd = &cobra.Command{
		Use:   "file",
		Short: "Start the worker with file sink",
		Run:   runWorker,
	}

	postgresWorkerCmd = &cobra.Command{
		Use:   "postgres",
		Short: "Start the worker with postgres sink",
		Run:   runWorker,
	}

	snowflakeWorkerCmd = &cobra.Command{
		Use:   "snowflake",
		Short: "Start the worker with Snowflake sink",
		Run:   runWorker,
	}

	webhookWorkerCmd = &cobra.Command{
		Use:   "webhook",
		Short: "Start the worker with webhook sink",
		Run:   runWorker,
	}

	// Version information set by goreleaser
	version = "dev"
	commit  = "none"
	date    = "unknown"

	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("pg_flo %s (commit: %s, built: %s)\n", version, commit, date)
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	})

	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.pg_flo.yaml)")

	addReplicatorFlags(replicatorCmd)
	addWorkerFlags(workerCmd)

	workerCmd.AddCommand(stdoutWorkerCmd, fileWorkerCmd, postgresWorkerCmd, webhookWorkerCmd, snowflakeWorkerCmd)
	rootCmd.AddCommand(replicatorCmd, workerCmd, versionCmd)

	cobra.OnInitialize(func() {
		if len(os.Args) > 1 && os.Args[1] != "version" && cfgFile == "" {
			markFlagRequired(replicatorCmd,
				"host", "port", "dbname", "user", "password", "group", "nats-url",
			)

			markFlagRequired(workerCmd,
				"group", "nats-url",
			)

			markFlagRequired(fileWorkerCmd, "file-output-dir")
			markFlagRequired(postgresWorkerCmd,
				"target-host", "target-dbname", "target-user", "target-password",
				"source-host", "source-dbname", "source-user", "source-password",
			)
			markFlagRequired(webhookWorkerCmd, "webhook-url")
			markFlagRequired(snowflakeWorkerCmd,
				"snowflake-account", "snowflake-user", "snowflake-password",
				"snowflake-warehouse", "snowflake-database", "snowflake-schema",
				"source-host", "source-dbname", "source-user", "source-password",
			)
		}
	})
}

// New function to add replicator flags without marking them required
func addReplicatorFlags(cmd *cobra.Command) {
	cmd.Flags().String("host", "", "PostgreSQL host (env: PG_FLO_HOST)")
	cmd.Flags().Int("port", 5432, "PostgreSQL port (env: PG_FLO_PORT)")
	cmd.Flags().String("dbname", "", "PostgreSQL database name (env: PG_FLO_DBNAME)")
	cmd.Flags().String("user", "", "PostgreSQL user (env: PG_FLO_USER)")
	cmd.Flags().String("password", "", "PostgreSQL password (env: PG_FLO_PASSWORD)")
	cmd.Flags().String("group", "", "Group name for replication (env: PG_FLO_GROUP)")
	cmd.Flags().String("schema", "public", "PostgreSQL schema (env: PG_FLO_SCHEMA)")
	cmd.Flags().StringSlice("tables", []string{}, "Tables to replicate (env: PG_FLO_TABLES)")
	cmd.Flags().String("nats-url", "", "NATS server URL (env: PG_FLO_NATS_URL)")
	cmd.Flags().Bool("stream", false, "Enable stream mode (default if no mode specified)")
	cmd.Flags().Bool("copy-and-stream", false, "Enable copy and stream mode")
	cmd.Flags().Bool("copy", false, "Enable copy mode without streaming")
	cmd.Flags().Int("max-copy-workers-per-table", 4, "Maximum number of copy workers per table (env: PG_FLO_MAX_COPY_WORKERS_PER_TABLE)")
	cmd.Flags().Bool("track-ddl", false, "Enable tracking of DDL changes (env: PG_FLO_TRACK_DDL)")
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".pg_flo")
	}

	viper.SetEnvPrefix("PG_FLO")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if err := viper.ReadInConfig(); err != nil {
		if cfgFile != "" {
			log.Fatal().Err(err).Msg("Failed to read config file")
		}
	} else {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	// Bind all flags to viper
	bindFlags(replicatorCmd)
	bindFlags(workerCmd)
	bindFlags(stdoutWorkerCmd)
	bindFlags(fileWorkerCmd)
	bindFlags(postgresWorkerCmd)
	bindFlags(webhookWorkerCmd)

	validateRequiredConfigs()
}

func validateRequiredConfigs() {
	var missingConfigs []string

	cmd, _, err := rootCmd.Find(os.Args[1:])
	if err != nil {
		return
	}

	var requiredConfigs []string
	switch cmd.Name() {
	case "replicator":
		requiredConfigs = []string{
			"host", "port", "dbname", "user", "password", "group", "nats-url",
		}
	case "postgres":
		requiredConfigs = []string{
			"group", "nats-url",
			"target-host", "target-dbname", "target-user", "target-password",
			"source-host", "source-dbname", "source-user", "source-password",
		}
	case "file":
		requiredConfigs = []string{"group", "nats-url", "file-output-dir"}
	case "webhook":
		requiredConfigs = []string{"group", "nats-url", "webhook-url"}
	}

	for _, req := range requiredConfigs {
		if !viper.IsSet(req) {
			missingConfigs = append(missingConfigs, req)
		}
	}

	if len(missingConfigs) > 0 {
		log.Fatal().Msgf("Missing required configurations: %s", strings.Join(missingConfigs, ", "))
	}
}

func bindFlags(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		envVar := fmt.Sprintf("PG_FLO_%s", strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_")))
		if err := viper.BindEnv(f.Name, envVar); err != nil {
			fmt.Printf("Error binding flag %s to env var: %v\n", f.Name, err)
		}

		if err := viper.BindPFlag(f.Name, f); err != nil {
			fmt.Printf("Error binding flag %s: %v\n", f.Name, err)
		}
	})
}

func runReplicator(_ *cobra.Command, _ []string) {
	var missingConfigs []string

	requiredConfigs := []string{"host", "port", "dbname", "user", "password", "group", "nats-url"}
	for _, req := range requiredConfigs {
		if !viper.IsSet(req) {
			missingConfigs = append(missingConfigs, req)
		}
	}

	if len(missingConfigs) > 0 {
		log.Fatal().Msgf("Missing required configurations: %s", strings.Join(missingConfigs, ", "))
	}

	config := replicator.Config{
		Host:     viper.GetString("host"),
		Port:     uint16(viper.GetInt("port")),
		Database: viper.GetString("dbname"),
		User:     viper.GetString("user"),
		Password: viper.GetString("password"),
		Group:    viper.GetString("group"),
		Schema:   viper.GetString("schema"),
		Tables:   viper.GetStringSlice("tables"),
		TrackDDL: viper.GetBool("track-ddl"),
	}

	natsURL := viper.GetString("nats-url")
	if natsURL == "" {
		log.Fatal().Msg("NATS URL is required")
	}

	natsClient, err := pgflonats.NewNATSClient(natsURL, fmt.Sprintf("pgflo_%s_stream", config.Group), config.Group)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create NATS client")
	}

	stream := viper.GetBool("stream")
	copyAndStream := viper.GetBool("copy-and-stream")
	copyMode := viper.GetBool("copy")

	modesSelected := 0
	if stream {
		modesSelected++
	}
	if copyAndStream {
		modesSelected++
	}
	if copyMode {
		modesSelected++
	}

	if modesSelected > 1 {
		log.Fatal().Msg("Cannot specify multiple modes: use --stream, --copy-and-stream, or --copy")
	}

	maxCopyWorkersPerTable := viper.GetInt("max-copy-workers-per-table")

	var factory replicator.Factory

	if copyMode {
		factory = &replicator.CopyAndStreamReplicatorFactory{
			MaxCopyWorkersPerTable: maxCopyWorkersPerTable,
			CopyOnly:               true,
		}
	} else if copyAndStream {
		factory = &replicator.CopyAndStreamReplicatorFactory{
			MaxCopyWorkersPerTable: maxCopyWorkersPerTable,
		}
	} else {
		factory = &replicator.StreamReplicatorFactory{}
	}

	// Create base context for the entire application
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rep, err := factory.CreateReplicator(config, natsClient)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create replicator")
	}

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Error channel to capture any replication errors
	errCh := make(chan error, 1)

	// Start replication in a goroutine
	go func() {
		if err := rep.Start(ctx); err != nil {
			errCh <- err
		}
	}()

	// Wait for either a signal or an error
	select {
	case sig := <-sigCh:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")

		// Create a new context with timeout for graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		// Cancel the main context first
		cancel()

		// Then call Stop with the timeout context
		if err := rep.Stop(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("Error during replication shutdown")
			os.Exit(1)
		}
		log.Info().Msg("Replication stopped successfully")

	case err := <-errCh:
		log.Error().Err(err).Msg("Replication error occurred")

		// Create shutdown context for cleanup
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		// Attempt cleanup even if there was an error
		if stopErr := rep.Stop(shutdownCtx); stopErr != nil {
			log.Error().Err(stopErr).Msg("Additional error during shutdown")
		}
		os.Exit(1)
	}
}

func runWorker(cmd *cobra.Command, _ []string) {
	group := viper.GetString("group")
	natsURL := viper.GetString("nats-url")
	if natsURL == "" {
		log.Fatal().Msg("NATS URL is required")
	}

	rulesConfigPath := viper.GetString("rules-config")
	routingConfigPath := viper.GetString("routing-config")
	sinkType := cmd.Use

	natsClient, err := pgflonats.NewNATSClient(natsURL, fmt.Sprintf("pgflo_%s_stream", group), group)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create NATS client")
	}

	ruleEngine := rules.NewRuleEngine()
	if rulesConfigPath != "" {
		rulesConfig, err := loadRulesConfig(rulesConfigPath)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load rules configuration")
		}
		if err := ruleEngine.LoadRules(rulesConfig); err != nil {
			log.Fatal().Err(err).Msg("Failed to load rules")
		}
	}

	router := routing.NewRouter()
	if routingConfigPath != "" {
		routingConfig, err := loadRoutingConfig(routingConfigPath)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to load routing configuration")
		}
		if err := router.LoadRoutes(routingConfig); err != nil {
			log.Fatal().Err(err).Msg("Failed to load routes")
		}
	}

	sink, err := createSink(sinkType)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create sink")
	}

	w := worker.NewWorker(
		natsClient,
		ruleEngine,
		router,
		sink,
		group,
		worker.WithBatchSize(viper.GetInt("batch-size")),
	)

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	log.Info().Msg("Starting worker...")
	if err := w.Start(ctx); err != nil {
		if err == context.Canceled {
			log.Info().Msg("Worker shut down gracefully")
		} else {
			log.Error().Err(err).Msg("Worker encountered an error during shutdown")
		}
	}
}

func loadRulesConfig(filePath string) (rules.Config, error) {
	var config rules.Config
	data, err := os.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

func loadRoutingConfig(filePath string) (map[string]routing.TableRoute, error) {
	var config map[string]routing.TableRoute
	data, err := os.ReadFile(filePath)
	if err != nil {
		return config, fmt.Errorf("failed to read routing config file: %w", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal routing config: %w", err)
	}
	return config, nil
}

func createSink(sinkType string) (sinks.Sink, error) {
	switch sinkType {
	case "stdout":
		return sinks.NewStdoutSink()
	case "file":
		return sinks.NewFileSink(
			viper.GetString("file-output-dir"),
		)
	case "postgres":
		return sinks.NewPostgresSink(
			viper.GetString("target-host"),
			viper.GetInt("target-port"),
			viper.GetString("target-dbname"),
			viper.GetString("target-user"),
			viper.GetString("target-password"),
			viper.GetBool("target-sync-schema"),
			viper.GetString("source-host"),
			viper.GetInt("source-port"),
			viper.GetString("source-dbname"),
			viper.GetString("source-user"),
			viper.GetString("source-password"),
			viper.GetBool("target-disable-foreign-keys"),
		)
	case "webhook":
		return sinks.NewWebhookSink(
			viper.GetString("webhook-url"),
		)
	case "snowflake":
		return sinks.NewSnowflakeSink(
			viper.GetString("snowflake-account"),
			viper.GetString("snowflake-user"),
			viper.GetString("snowflake-password"),
			viper.GetString("snowflake-role"),
			viper.GetString("snowflake-warehouse"),
			viper.GetString("snowflake-database"),
			viper.GetString("snowflake-schema"),
			viper.GetString("source-host"),
			viper.GetInt("source-port"),
			viper.GetString("source-dbname"),
			viper.GetString("source-user"),
			viper.GetString("source-password"),
		)
	default:
		return nil, fmt.Errorf("unknown sink type: %s", sinkType)
	}
}

// Helper function to mark multiple flags as required
func markFlagRequired(cmd *cobra.Command, flags ...string) {
	for _, flag := range flags {
		if err := cmd.MarkFlagRequired(flag); err != nil {
			fmt.Printf("Error marking flag %s as required: %v\n", flag, err)
		}
	}
}

// Add worker flags
func addWorkerFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("group", "", "Group name for worker (env: PG_FLO_GROUP)")
	cmd.PersistentFlags().String("nats-url", "", "NATS server URL (env: PG_FLO_NATS_URL)")
	cmd.PersistentFlags().String("rules-config", "", "Path to rules configuration file (env: PG_FLO_RULES_CONFIG)")
	cmd.PersistentFlags().String("routing-config", "", "Path to routing configuration file (env: PG_FLO_ROUTING_CONFIG)")
	cmd.PersistentFlags().Int("batch-size", 1000, "Batch size for processing messages (env: PG_FLO_BATCH_SIZE)")

	// File sink specific flags
	fileWorkerCmd.Flags().String("file-output-dir", "", "Output directory for file sink (env: PG_FLO_FILE_OUTPUT_DIR)")

	// Postgres sink specific flags
	postgresWorkerCmd.Flags().String("target-host", "", "Target PostgreSQL host (env: PG_FLO_TARGET_HOST)")
	postgresWorkerCmd.Flags().Int("target-port", 5432, "Target PostgreSQL port (env: PG_FLO_TARGET_PORT)")
	postgresWorkerCmd.Flags().String("target-dbname", "", "Target PostgreSQL database name (env: PG_FLO_TARGET_DBNAME)")
	postgresWorkerCmd.Flags().String("target-user", "", "Target PostgreSQL user (env: PG_FLO_TARGET_USER)")
	postgresWorkerCmd.Flags().String("target-password", "", "Target PostgreSQL password (env: PG_FLO_TARGET_PASSWORD)")
	postgresWorkerCmd.Flags().Bool("target-sync-schema", false, "Sync schema to target (env: PG_FLO_TARGET_SYNC_SCHEMA)")
	postgresWorkerCmd.Flags().Bool("target-disable-foreign-keys", false, "Disable foreign key constraints on target (env: PG_FLO_TARGET_DISABLE_FOREIGN_KEYS)")

	// Source database connection for schema sync
	postgresWorkerCmd.Flags().String("source-host", "", "Source PostgreSQL host (env: PG_FLO_SOURCE_HOST)")
	postgresWorkerCmd.Flags().Int("source-port", 5432, "Source PostgreSQL port (env: PG_FLO_SOURCE_PORT)")
	postgresWorkerCmd.Flags().String("source-dbname", "", "Source PostgreSQL database name (env: PG_FLO_SOURCE_DBNAME)")
	postgresWorkerCmd.Flags().String("source-user", "", "Source PostgreSQL user (env: PG_FLO_SOURCE_USER)")
	postgresWorkerCmd.Flags().String("source-password", "", "Source PostgreSQL password (env: PG_FLO_SOURCE_PASSWORD)")

	// Webhook sink specific flags
	webhookWorkerCmd.Flags().String("webhook-url", "", "Webhook URL (env: PG_FLO_WEBHOOK_URL)")

	// Snowflake sink specific flags
	snowflakeWorkerCmd.Flags().String("snowflake-account", "", "Snowflake account (env: PG_FLO_SNOWFLAKE_ACCOUNT)")
	snowflakeWorkerCmd.Flags().String("snowflake-user", "", "Snowflake user (env: PG_FLO_SNOWFLAKE_USER)")
	snowflakeWorkerCmd.Flags().String("snowflake-password", "", "Snowflake password (env: PG_FLO_SNOWFLAKE_PASSWORD)")
	snowflakeWorkerCmd.Flags().String("snowflake-role", "ACCOUNTADMIN", "Snowflake role (env: PG_FLO_SNOWFLAKE_ROLE)")
	snowflakeWorkerCmd.Flags().String("snowflake-warehouse", "", "Snowflake warehouse (env: PG_FLO_SNOWFLAKE_WAREHOUSE)")
	snowflakeWorkerCmd.Flags().String("snowflake-database", "", "Snowflake database (env: PG_FLO_SNOWFLAKE_DATABASE)")
	snowflakeWorkerCmd.Flags().String("snowflake-schema", "PUBLIC", "Snowflake schema (env: PG_FLO_SNOWFLAKE_SCHEMA)")

	// Add source database connection flags for Snowflake
	snowflakeWorkerCmd.Flags().String("source-host", "", "Source PostgreSQL host (env: PG_FLO_SOURCE_HOST)")
	snowflakeWorkerCmd.Flags().Int("source-port", 5432, "Source PostgreSQL port (env: PG_FLO_SOURCE_PORT)")
	snowflakeWorkerCmd.Flags().String("source-dbname", "", "Source PostgreSQL database name (env: PG_FLO_SOURCE_DBNAME)")
	snowflakeWorkerCmd.Flags().String("source-user", "", "Source PostgreSQL user (env: PG_FLO_SOURCE_USER)")
	snowflakeWorkerCmd.Flags().String("source-password", "", "Source PostgreSQL password (env: PG_FLO_SOURCE_PASSWORD)")
}
