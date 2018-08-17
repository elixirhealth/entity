package server

import (
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
)

// Entity implements the EntityServer interface.
type Entity struct {
	*server.BaseServer
	config *Config

	storer storage.Storer
	// TODO maybe add other things here
}

// newEntity creates a new EntityServer from the given config.
func newEntity(config *Config) (*Entity, error) {
	baseServer := server.NewBaseServer(config.BaseConfig)
	storer, err := getStorer(config, baseServer.Logger)
	if err != nil {
		return nil, err
	}
	// TODO maybe add other init

	return &Entity{
		BaseServer: baseServer,
		config:     config,
		storer:     storer,
		// TODO maybe add other things
	}, nil
}

// TODO implement entityapi.Entity endpoints
