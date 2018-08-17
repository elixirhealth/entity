package server

import (
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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

func (e *Entity) PutEntity(
	ctx context.Context, rq *api.PutEntityRequest,
) (*api.PutEntityResponse, error) {
	e.Logger.Debug("received PutEntity request", logPutEntityRq(rq)...)
	if err := api.ValidatePutEntityRequest(rq); err != nil {
		return nil, err
	}
	newEntity := rq.Entity.EntityId == ""
	entityID, err := e.storer.PutEntity(rq.Entity)
	if err != nil {
		return nil, err
	}
	rp := &api.PutEntityResponse{EntityId: entityID}
	e.Logger.Info("put entity", logPutEntityRp(rq, rp, newEntity)...)
	return rp, nil
}

func (e *Entity) GetEntity(
	ctx context.Context, rq *api.GetEntityRequest,
) (*api.GetEntityResponse, error) {
	e.Logger.Debug("received GetEntity request", zap.String(logEntityID, rq.EntityId))
	if err := api.ValidateGetEntityRequest(rq); err != nil {
		return nil, err
	}
	ent, err := e.storer.GetEntity(rq.EntityId)
	if err != nil {
		return nil, err
	}
	rp := &api.GetEntityResponse{Entity: ent}
	e.Logger.Info("got entity", logGetEntityRp(rp)...)
	return rp, nil
}

func (e *Entity) SearchEntity(
	ctx context.Context, rq *api.SearchEntityRequest,
) (*api.SearchEntityResponse, error) {
	e.Logger.Debug("received SearchEntity request", logSearchEntityRq(rq)...)
	if err := api.ValidateSearchEntityRequest(rq); err != nil {
		return nil, err
	}
	es, err := e.storer.SearchEntity(rq.Query, uint(rq.Limit))
	if err != nil {
		return nil, err
	}
	rp := &api.SearchEntityResponse{Entities: es}
	if len(rp.Entities) == 0 {
		e.Logger.Info("found no entities", logSearchEntityRp(rq, rp)...)
	} else {
		e.Logger.Info("found entities", logSearchEntityRp(rq, rp)...)
	}
	return rp, nil
}

func (e *Entity) AddPublicKeys(
	ctx context.Context, rq *api.AddPublicKeysRequest,
) (*api.AddPublicKeysResponse, error) {
	panic("implement me")
}

func (e *Entity) GetPublicKeys(
	ctx context.Context, rq *api.GetPublicKeysRequest,
) (*api.GetPublicKeysResponse, error) {
	panic("implement me")
}

func (e *Entity) SamplePublicKeys(
	ctx context.Context, rq *api.SamplePublicKeysRequest,
) (*api.SamplePublicKeysResponse, error) {
	panic("implement me")
}

func (e *Entity) GetPublicKeyDetails(
	ctx context.Context, rq *api.GetPublicKeyDetailsRequest,
) (*api.GetPublicKeyDetailsResponse, error) {
	panic("implement me")
}
