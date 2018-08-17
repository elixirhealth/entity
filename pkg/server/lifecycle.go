package server

import (
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"google.golang.org/grpc"
)

// Start starts the server and eviction routines.
func Start(config *Config, up chan *Entity) error {
	c, err := newEntity(config)
	if err != nil {
		return err
	}

	// start Entity aux routines
	// TODO add go x.auxRoutine() or delete comment

	registerServer := func(s *grpc.Server) { api.RegisterEntityServer(s, c) }
	return c.Serve(registerServer, func() { up <- c })
}
