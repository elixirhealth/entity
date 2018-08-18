package cmd

import (
	"errors"
	"fmt"
	"log"
	"os"

	cerrors "github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	"github.com/elixirhealth/entity/pkg/server"
	"github.com/elixirhealth/entity/version"
	"github.com/elixirhealth/service-base/pkg/cmd"
	bserver "github.com/elixirhealth/service-base/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	serviceNameLower    = "entity"
	serviceNameCamel    = "Entity"
	envVarPrefix        = "ENTITY"
	logLevelFlag        = "logLevel"
	storageMemoryFlag   = "storageMemory"
	storagePostgresFlag = "storagePostgres"
	dbURLFlag           = "dbURL"
	dbPasswordFlag      = "dbPassword"
	nEntitiesFlag       = "nEntities"
	nSearchesFlag       = "nSearches"
)

var (
	errMultipleStorageTypes = errors.New("multiple storage types specified")
	errNoStorageType        = errors.New("no storage type specified")

	rootCmd = &cobra.Command{
		Short: "TODO", // TODO
	}
)

func init() {
	rootCmd.PersistentFlags().String(logLevelFlag, bserver.DefaultLogLevel.String(),
		"log level")

	cmd.Start(serviceNameLower, serviceNameCamel, rootCmd, version.Current, start,
		func(flags *pflag.FlagSet) {
			flags.Bool(storageMemoryFlag, true, "use in-memory storage")
			flags.Bool(storagePostgresFlag, false, "use Postgres DB storage")
			flags.String(dbURLFlag, "", "Postgres DB URL")
		})

	testCmd := cmd.Test(serviceNameLower, rootCmd)
	cmd.TestHealth(serviceNameLower, testCmd)
	cmd.TestIO(serviceNameLower, testCmd, testIO, func(flags *pflag.FlagSet) {
		flags.Uint(nEntitiesFlag, 32, "number of test entities to create")
		flags.Uint(nSearchesFlag, 16, "number of test searches to perform")
	})

	cmd.Version(serviceNameLower, rootCmd, version.Current)

	// bind viper flags
	viper.SetEnvPrefix(envVarPrefix) // look for env vars with prefix
	viper.AutomaticEnv()             // read in environment variables that match
	cerrors.MaybePanic(viper.BindPFlags(rootCmd.Flags()))
}

// Execute runs the root entity command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func start() error {
	writeBanner(os.Stdout)
	config, err := getEntityConfig()
	if err != nil {
		return err
	}
	return server.Start(config, make(chan *server.Entity, 1))
}

func getEntityConfig() (*server.Config, error) {
	storageType, err := getStorageType()
	if err != nil {
		return nil, err
	}
	c := server.NewDefaultConfig()
	c.WithServerPort(uint(viper.GetInt(cmd.ServerPortFlag))).
		WithMetricsPort(uint(viper.GetInt(cmd.MetricsPortFlag))).
		WithProfilerPort(uint(viper.GetInt(cmd.ProfilerPortFlag))).
		WithLogLevel(logging.GetLogLevel(viper.GetString(logLevelFlag))).
		WithProfile(viper.GetBool(cmd.ProfileFlag))

	c.Storage.Type = storageType
	c.DBUrl = getDBUrl()

	lg := logging.NewDevLogger(c.LogLevel)
	lg.Info("successfully parsed config", zap.Object("config", c))

	return c, nil
}

func getDBUrl() string {
	dbURL := viper.GetString(dbURLFlag)
	if dbPass := viper.GetString(dbPasswordFlag); dbPass != "" {
		// append pw to URL args
		return fmt.Sprintf("%s&password=%s", dbURL, dbPass)
	}
	return dbURL
}

func getStorageType() (bstorage.Type, error) {
	if viper.GetBool(storageMemoryFlag) && viper.GetBool(storagePostgresFlag) {
		return bstorage.Unspecified, errMultipleStorageTypes
	}
	if viper.GetBool(storageMemoryFlag) {
		return bstorage.Memory, nil
	}
	if viper.GetBool(storagePostgresFlag) {
		return bstorage.Postgres, nil
	}
	return bstorage.Unspecified, errNoStorageType
}
