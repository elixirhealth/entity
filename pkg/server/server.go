package server

import (
	"math/rand"
	"time"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrTooManyActivePublicKeys indicates when adding more public keys would bring the total
	// number of active PKs abvoe the maximum allowed.
	ErrTooManyActivePublicKeys = status.Error(codes.FailedPrecondition,
		"too many active public keys for the entity and key type")

	// ErrInternal represents an internal error (e.g., with storage or dependency service call).
	ErrInternal = status.Error(codes.Internal, "internal error")
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
	return &Entity{
		BaseServer: baseServer,
		config:     config,
		storer:     storer,
	}, nil
}

func (e *Entity) PutEntity(
	ctx context.Context, rq *api.PutEntityRequest,
) (*api.PutEntityResponse, error) {
	e.Logger.Debug("received PutEntity request", logPutEntityRq(rq)...)
	if err := api.ValidatePutEntityRequest(rq); err != nil {
		return nil, wrapErr(codes.InvalidArgument, err)
	}
	newEntity := rq.Entity.EntityId == ""
	entityID, err := e.storer.PutEntity(rq.Entity)
	if err != nil {
		return nil, ErrInternal
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
		return nil, wrapErr(codes.InvalidArgument, err)
	}
	ent, err := e.storer.GetEntity(rq.EntityId)
	if err != nil {
		return nil, ErrInternal
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
		return nil, wrapErr(codes.InvalidArgument, err)
	}
	es, err := e.storer.SearchEntity(rq.Query, uint(rq.Limit))
	if err != nil {
		return nil, ErrInternal
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
	e.Logger.Debug("received add public keys request", logAddPublicKeysRq(rq)...)
	if err := api.ValidateAddPublicKeysRequest(rq); err != nil {
		e.Logger.Info("add public keys request invalid", zap.String(logErr, err.Error()))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if n, err := e.storer.CountEntityPublicKeys(rq.EntityId, rq.KeyType); err != nil {
		e.Logger.Error("storer count entity public keys error", zap.Error(err))
		return nil, ErrInternal
	} else if n+len(rq.PublicKeys) > storage.MaxEntityKeyTypeKeys {
		return nil, ErrTooManyActivePublicKeys
	}
	pkds := getPublicKeyDetails(rq)
	if err := e.storer.AddPublicKeys(pkds); err != nil {
		e.Logger.Error("storer add public keys error", zap.Error(err))
		return nil, ErrInternal
	}
	e.Logger.Info("added public keys", logAddPublicKeysRq(rq)...)
	return &api.AddPublicKeysResponse{}, nil
}

// GetPublicKeys returns the public keys of a given type for a given entity ID.
func (e *Entity) GetPublicKeys(
	ctx context.Context, rq *api.GetPublicKeysRequest,
) (*api.GetPublicKeysResponse, error) {
	e.Logger.Debug("received get public keys request", logGetPublicKeysRq(rq)...)
	if err := api.ValidateGetPublicKeysRequest(rq); err != nil {
		e.Logger.Info("get public keys request invalid", zap.String(logErr, err.Error()))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pkds, err := e.storer.GetEntityPublicKeys(rq.EntityId, rq.KeyType)
	if err != nil {
		e.Logger.Error("storer get entity public keys error", zap.Error(err))
		return nil, ErrInternal
	}
	pks := make([][]byte, len(pkds))
	for i, pkd := range pkds {
		pks[i] = pkd.PublicKey
	}
	rp := &api.GetPublicKeysResponse{PublicKeys: pks}
	e.Logger.Info("got public keys", logGetPublicKeysRp(rq, rp)...)
	return rp, nil
}

// GetPublicKeyDetails gets the details (including their associated entity IDs) for a given set of
// public keys.
func (e *Entity) GetPublicKeyDetails(
	ctx context.Context, rq *api.GetPublicKeyDetailsRequest,
) (*api.GetPublicKeyDetailsResponse, error) {
	e.Logger.Debug("received get public key details request",
		zap.Int(logNKeys, len(rq.PublicKeys)))
	if err := api.ValidateGetPublicKeyDetailsRequest(rq); err != nil {
		e.Logger.Info("get public key details request invalid",
			zap.String(logErr, err.Error()))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pkds, err := e.storer.GetPublicKeys(rq.PublicKeys)
	if err != nil && err == api.ErrNoSuchPublicKey {
		return nil, status.Error(codes.NotFound, err.Error())
	} else if err != nil {
		e.Logger.Error("storer get public keys error", zap.Error(err))
		return nil, ErrInternal
	}
	e.Logger.Info("got public key details", zap.Int(logNKeys, len(pkds)))
	return &api.GetPublicKeyDetailsResponse{
		PublicKeyDetails: pkds,
	}, nil
}

// SamplePublicKeys returns a sample of public keys of the given entity.
func (e *Entity) SamplePublicKeys(
	ctx context.Context, rq *api.SamplePublicKeysRequest,
) (*api.SamplePublicKeysResponse, error) {
	e.Logger.Debug("received sample public keys request", logSamplePublicKeysRq(rq)...)
	if err := api.ValidateSamplePublicKeysRequest(rq); err != nil {
		e.Logger.Info("sample public keys request invalid", zap.String(logErr, err.Error()))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	allPKDs, err := e.storer.GetEntityPublicKeys(rq.OfEntityId, api.KeyType_READER)
	if err != nil {
		e.Logger.Error("storer get entity public keys error", zap.Error(err))
		return nil, ErrInternal
	}
	orderKey := []byte(rq.RequesterEntityId)
	topOrdered := getOrderedLimit(allPKDs, orderKey, api.MaxSamplePublicKeysSize)
	rng := rand.New(rand.NewSource(int64(time.Now().Nanosecond()))) // good enough
	topSampled := sampleWithoutReplacement(topOrdered, rng, int(rq.NPublicKeys))
	rp := &api.SamplePublicKeysResponse{
		PublicKeyDetails: topSampled,
	}
	e.Logger.Info("sampled public keys", logSamplePublicKeysRp(rq, rp)...)
	return rp, nil
}
