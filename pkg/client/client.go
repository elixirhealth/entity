package client

import (
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"google.golang.org/grpc"
)

// NewInsecure returns a new DirectoryClient without any TLS on the connection.
func NewInsecure(address string) (api.EntityClient, error) {
	cc, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return api.NewEntityClient(cc), nil
}
