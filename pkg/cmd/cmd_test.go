package cmd

import (
	"testing"

	"github.com/elixirhealth/service-base/pkg/cmd"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestGetEntityConfig(t *testing.T) {
	serverPort := uint(1234)
	metricsPort := uint(5678)
	profilerPort := uint(9012)
	logLevel := zapcore.DebugLevel.String()
	profile := true
	dbURL := "some URL"
	storageMemory := false
	storagePostgres := true

	viper.Set(cmd.ServerPortFlag, serverPort)
	viper.Set(cmd.MetricsPortFlag, metricsPort)
	viper.Set(cmd.ProfilerPortFlag, profilerPort)
	viper.Set(cmd.LogLevelFlag, logLevel)
	viper.Set(cmd.ProfileFlag, profile)
	viper.Set(dbURLFlag, dbURL)
	viper.Set(storageMemoryFlag, storageMemory)
	viper.Set(storagePostgresFlag, storagePostgres)

	c, err := getEntityConfig()
	assert.Nil(t, err)
	assert.Equal(t, serverPort, c.ServerPort)
	assert.Equal(t, metricsPort, c.MetricsPort)
	assert.Equal(t, profilerPort, c.ProfilerPort)
	assert.Equal(t, logLevel, c.LogLevel.String())
	assert.Equal(t, profile, c.Profile)
	assert.Equal(t, dbURL, c.DBUrl)
	assert.Equal(t, bstorage.Postgres, c.Storage.Type)
}

func TestGetStorageType(t *testing.T) {
	viper.Set(storageMemoryFlag, true)
	viper.Set(storagePostgresFlag, false)
	st, err := getStorageType()
	assert.Nil(t, err)
	assert.Equal(t, bstorage.Memory, st)

	viper.Set(storageMemoryFlag, false)
	viper.Set(storagePostgresFlag, true)
	st, err = getStorageType()
	assert.Nil(t, err)
	assert.Equal(t, bstorage.Postgres, st)

	viper.Set(storageMemoryFlag, true)
	viper.Set(storagePostgresFlag, true)
	st, err = getStorageType()
	assert.Equal(t, errMultipleStorageTypes, err)
	assert.Equal(t, bstorage.Unspecified, st)

	viper.Set(storageMemoryFlag, false)
	viper.Set(storagePostgresFlag, false)
	st, err = getStorageType()
	assert.Equal(t, errNoStorageType, err)
	assert.Equal(t, bstorage.Unspecified, st)
}
