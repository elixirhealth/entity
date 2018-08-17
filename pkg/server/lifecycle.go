package server

import (
	"github.com/drausin/libri/libri/common/errors"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage/postgres/migrations"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/mattes/migrate/source/go-bindata"
	"google.golang.org/grpc"
)

// Start starts the server and eviction routines.
func Start(config *Config, up chan *Entity) error {
	d, err := newEntity(config)
	if err != nil {
		return err
	}

	if err := d.maybeMigrateDB(); err != nil {
		return err
	}

	registerServer := func(s *grpc.Server) { api.RegisterEntityServer(s, d) }
	return d.Serve(registerServer, func() { up <- d })
}

// StopServer handles cleanup involved in closing down the server.
func (e *Entity) StopServer() {
	e.BaseServer.StopServer()
	err := e.storer.Close()
	errors.MaybePanic(err)
}

func (e *Entity) maybeMigrateDB() error {
	if e.config.Storage.Type != bstorage.Postgres {
		return nil
	}

	m := bstorage.NewBindataMigrator(
		e.config.DBUrl,
		bindata.Resource(migrations.AssetNames(), migrations.Asset),
		&bstorage.ZapLogger{Logger: e.Logger},
	)
	return m.Up()
}
