package server

import (
	"errors"
	"math/rand"
	"testing"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
	bserver "github.com/elixirhealth/service-base/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errTest  = errors.New("some test error")
	okEntity = api.NewTestPatient(0, false)
)

func TestNewEntity_ok(t *testing.T) {
	config := NewDefaultConfig().WithDBUrl("some DB URL")
	c, err := newEntity(config)
	assert.Nil(t, err)
	assert.NotEmpty(t, c.storer)
	assert.Equal(t, config, c.config)
}

func TestNewEntity_err(t *testing.T) {
	badConfigs := map[string]*Config{
		"empty DBUrl": NewDefaultConfig().
			WithDBUrl("").
			WithStorage(&storage.Parameters{Type: bstorage.Postgres}),
	}
	for desc, badConfig := range badConfigs {
		c, err := newEntity(badConfig)
		assert.NotNil(t, err, desc)
		assert.Nil(t, c)
	}
}

func TestEntity_PutEntity_ok(t *testing.T) {
	d := &Entity{
		BaseServer: server.NewBaseServer(server.NewDefaultBaseConfig()),
		storer: &fixedStorer{
			putEntityID: "some entity ID",
		},
	}
	rq := &api.PutEntityRequest{
		Entity: okEntity,
	}

	rp, err := d.PutEntity(context.Background(), rq)
	assert.Nil(t, err)
	assert.NotEmpty(t, rp.EntityId)
}

func TestEntity_PutEntity_err(t *testing.T) {
	baseServer := server.NewBaseServer(server.NewDefaultBaseConfig())
	cases := map[string]struct {
		d        *Entity
		rq       *api.PutEntityRequest
		expected error
	}{
		"invalid request": {
			d: &Entity{
				BaseServer: baseServer,
			},
			rq:       &api.PutEntityRequest{},
			expected: wrapErr(codes.InvalidArgument, api.ErrPutMissingEntity),
		},
		"storer Put error": {
			d: &Entity{
				BaseServer: baseServer,
				storer: &fixedStorer{
					putErr: errors.New("some Put error"),
				},
			},
			rq: &api.PutEntityRequest{
				Entity: okEntity,
			},
			expected: ErrInternal,
		},
	}

	for desc, c := range cases {
		rp, err := c.d.PutEntity(context.Background(), c.rq)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, rp, desc)
	}
}

func TestEntity_GetEntity_ok(t *testing.T) {
	d := &Entity{
		BaseServer: server.NewBaseServer(server.NewDefaultBaseConfig()),
		storer: &fixedStorer{
			getEntity: okEntity,
		},
	}
	rq := &api.GetEntityRequest{
		EntityId: "some entity ID",
	}

	rp, err := d.GetEntity(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, okEntity, rp.Entity)
}

func TestEntity_GetEntity_err(t *testing.T) {
	baseServer := server.NewBaseServer(server.NewDefaultBaseConfig())
	cases := map[string]struct {
		d        *Entity
		rq       *api.GetEntityRequest
		expected error
	}{
		"invalid request": {
			d: &Entity{
				BaseServer: baseServer,
			},
			rq:       &api.GetEntityRequest{},
			expected: wrapErr(codes.InvalidArgument, api.ErrGetMissingEntityID),
		},
		"storer Get error": {
			d: &Entity{
				BaseServer: baseServer,
				storer: &fixedStorer{
					getErr: errors.New("some Get error"),
				},
			},
			rq: &api.GetEntityRequest{
				EntityId: "some entity ID",
			},
			expected: ErrInternal,
		},
	}

	for desc, c := range cases {
		rp, err := c.d.GetEntity(context.Background(), c.rq)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, rp, desc)
	}
}

func TestEntity_SearchEntity_ok(t *testing.T) {
	d := &Entity{
		BaseServer: server.NewBaseServer(server.NewDefaultBaseConfig()),
		storer: &fixedStorer{
			searchEntities: []*api.EntityDetail{
				api.NewTestPatient(0, true),
				api.NewTestPatient(1, true),
			},
		},
	}
	rq := &api.SearchEntityRequest{
		Query: "some query",
		Limit: 8,
	}

	rp, err := d.SearchEntity(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rp.Entities))
}

func TestEntity_SearchEntity_err(t *testing.T) {
	baseServer := server.NewBaseServer(server.NewDefaultBaseConfig())
	cases := map[string]struct {
		d        *Entity
		rq       *api.SearchEntityRequest
		expected error
	}{
		"invalid request": {
			d: &Entity{
				BaseServer: baseServer,
			},
			rq:       &api.SearchEntityRequest{},
			expected: wrapErr(codes.InvalidArgument, api.ErrSearchQueryTooShort),
		},
		"storer Search error": {
			d: &Entity{
				BaseServer: baseServer,
				storer: &fixedStorer{
					searchErr: errors.New("some Search error"),
				},
			},
			rq: &api.SearchEntityRequest{
				Query: "some query",
				Limit: 4,
			},
			expected: ErrInternal,
		},
	}

	for desc, c := range cases {
		rp, err := c.d.SearchEntity(context.Background(), c.rq)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, rp, desc)
	}
}

func TestEntity_AddPublicKeys_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer:     &fixedStorer{},
	}
	rq := &api.AddPublicKeysRequest{
		EntityId: "some entity ID",
		KeyType:  api.KeyType_READER,
		PublicKeys: [][]byte{
			util.RandBytes(rng, 33),
			util.RandBytes(rng, 33),
		},
	}
	rp, err := k.AddPublicKeys(context.Background(), rq)
	assert.Nil(t, err)
	assert.NotNil(t, rp)
}

func TestEntity_AddPublicKeys_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	baseServer := bserver.NewBaseServer(bserver.NewDefaultBaseConfig())
	okRq := &api.AddPublicKeysRequest{
		EntityId: "some entity ID",
		KeyType:  api.KeyType_READER,
		PublicKeys: [][]byte{
			util.RandBytes(rng, 33),
			util.RandBytes(rng, 33),
		},
	}
	cases := map[string]struct {
		k        *Entity
		rq       *api.AddPublicKeysRequest
		expected error
	}{
		"bad request": {
			k: &Entity{
				BaseServer: baseServer,
				storer:     &fixedStorer{},
			},
			rq:       &api.AddPublicKeysRequest{},
			expected: status.Error(codes.InvalidArgument, api.ErrEmptyEntityID.Error()),
		},
		"storer get count error": {
			k: &Entity{
				BaseServer: baseServer,
				storer:     &fixedStorer{countEntityPKsErr: errTest},
			},
			rq:       okRq,
			expected: ErrInternal,
		},
		"too many added": {
			k: &Entity{
				BaseServer: baseServer,
				storer:     &fixedStorer{countEntityPKsValue: 255},
			},
			rq:       okRq,
			expected: ErrTooManyActivePublicKeys,
		},
		"storer add error": {
			k: &Entity{
				BaseServer: baseServer,
				storer:     &fixedStorer{addErr: errTest},
			},
			rq:       okRq,
			expected: ErrInternal,
		},
	}
	for desc, c := range cases {
		rp, err := c.k.AddPublicKeys(context.Background(), c.rq)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, rp, desc)
	}
}

func TestEntity_GetPublicKeys_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	n := 2
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer: &fixedStorer{
			getEntityPKs: api.NewTestPublicKeyDetails(rng, n),
		},
	}
	rq := &api.GetPublicKeysRequest{EntityId: "some entity ID"}
	rp, err := k.GetPublicKeys(context.Background(), rq)
	assert.Nil(t, err)
	assert.NotNil(t, rp)
	assert.Equal(t, n, len(rp.PublicKeys))
}

func TestEntity_GetPublicKeys_err(t *testing.T) {
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer:     &fixedStorer{getEntityPKsErr: errTest},
	}

	// bad request
	rq := &api.GetPublicKeysRequest{}
	rp, err := k.GetPublicKeys(context.Background(), rq)
	assert.NotNil(t, err)
	assert.Nil(t, rp)

	// storer error
	rq = &api.GetPublicKeysRequest{EntityId: "some entity ID"}
	rp, err = k.GetPublicKeys(context.Background(), rq)
	assert.Equal(t, ErrInternal, err)
	assert.Nil(t, rp)
}

func TestEntity_GetPublicKeyDetails_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	pks := [][]byte{
		util.RandBytes(rng, 33),
		util.RandBytes(rng, 33),
	}
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer: &fixedStorer{
			getPKDs: api.NewTestPublicKeyDetails(rng, len(pks)),
		},
	}
	rq := &api.GetPublicKeyDetailsRequest{PublicKeys: pks}
	rp, err := k.GetPublicKeyDetails(context.Background(), rq)
	assert.Nil(t, err)
	assert.NotNil(t, rp)
	assert.Equal(t, len(pks), len(rp.PublicKeyDetails))
}

func TestEntity_GetPublicKeyDetails_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer:     &fixedStorer{getErr: errTest},
	}

	// bad request
	rq := &api.GetPublicKeyDetailsRequest{}
	rp, err := k.GetPublicKeyDetails(context.Background(), rq)
	assert.NotNil(t, err)
	assert.Nil(t, rp)

	// storer error
	rq = &api.GetPublicKeyDetailsRequest{
		PublicKeys: [][]byte{
			util.RandBytes(rng, 33),
			util.RandBytes(rng, 33),
		},
	}
	rp, err = k.GetPublicKeyDetails(context.Background(), rq)
	assert.Equal(t, ErrInternal, err)
	assert.Nil(t, rp)

	// no such pub key
	k = &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer:     &fixedStorer{getErr: api.ErrNoSuchPublicKey},
	}
	rp, err = k.GetPublicKeyDetails(context.Background(), rq)
	assert.NotNil(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, api.ErrNoSuchPublicKey.Error(), status.Convert(err).Message())
	assert.Nil(t, rp)
}

func TestEntity_SamplePublicKeys_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	nEntityPKDs := 64
	ctx := context.Background()
	ofEntityID, rqEntityID := "some entity ID", "another entity ID"
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer: &fixedStorer{
			getEntityPKs: api.NewTestPublicKeyDetails(rng, nEntityPKDs),
		},
	}

	rp1, err := k.SamplePublicKeys(ctx, &api.SamplePublicKeysRequest{
		OfEntityId:        ofEntityID,
		NPublicKeys:       2,
		RequesterEntityId: rqEntityID,
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rp1.PublicKeyDetails))

	// check sample again yields diff result
	rp2, err := k.SamplePublicKeys(ctx, &api.SamplePublicKeysRequest{
		OfEntityId:        ofEntityID,
		NPublicKeys:       2,
		RequesterEntityId: rqEntityID,
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rp2.PublicKeyDetails))
	assert.NotEqual(t, rp1.PublicKeyDetails, rp2.PublicKeyDetails)

	// check 2 samples of max public keys size yield same result
	rq := &api.SamplePublicKeysRequest{
		OfEntityId:        ofEntityID,
		NPublicKeys:       api.MaxSamplePublicKeysSize,
		RequesterEntityId: rqEntityID,
	}
	rp3, err := k.SamplePublicKeys(ctx, rq)
	assert.Nil(t, err)
	rp4, err := k.SamplePublicKeys(ctx, rq)
	assert.Nil(t, err)
	assert.Equal(t, rp3, rp4)

	// check another sample with diff requester has diff result
	rp5, err := k.SamplePublicKeys(ctx, &api.SamplePublicKeysRequest{
		OfEntityId:        ofEntityID,
		NPublicKeys:       api.MaxSamplePublicKeysSize,
		RequesterEntityId: "diff requester",
	})
	assert.Nil(t, err)
	assert.NotEqual(t, rp4, rp5)
}

func TestEntity_SamplePublicKeys_err(t *testing.T) {
	k := &Entity{
		BaseServer: bserver.NewBaseServer(bserver.NewDefaultBaseConfig()),
		storer:     &fixedStorer{getEntityPKsErr: errTest},
	}
	ofEntityID, rqEntityID := "some entity ID", "another entity ID"

	// bad request
	rq := &api.SamplePublicKeysRequest{}
	rp, err := k.SamplePublicKeys(context.Background(), rq)
	assert.NotNil(t, err)
	assert.Nil(t, rp)

	// storer error
	rq = &api.SamplePublicKeysRequest{
		OfEntityId:        ofEntityID,
		NPublicKeys:       api.MaxSamplePublicKeysSize,
		RequesterEntityId: rqEntityID,
	}
	rp, err = k.SamplePublicKeys(context.Background(), rq)
	assert.Equal(t, ErrInternal, err)
	assert.Nil(t, rp)
}

type fixedStorer struct {
	putEntityID         string
	putErr              error
	getEntity           *api.EntityDetail
	searchEntities      []*api.EntityDetail
	searchErr           error
	closeErr            error
	addErr              error
	getPKDs             []*api.PublicKeyDetail
	getErr              error
	countEntityPKsValue int
	countEntityPKsErr   error
	getEntityPKs        []*api.PublicKeyDetail
	getEntityPKsErr     error
}

func (f *fixedStorer) PutEntity(e *api.EntityDetail) (string, error) {
	return f.putEntityID, f.putErr
}

func (f *fixedStorer) GetEntity(entityID string) (*api.EntityDetail, error) {
	return f.getEntity, f.getErr
}

func (f *fixedStorer) SearchEntity(query string, limit uint) ([]*api.EntityDetail, error) {
	return f.searchEntities, f.searchErr
}

func (f *fixedStorer) CountEntityPublicKeys(entityID string, kt api.KeyType) (int, error) {
	return f.countEntityPKsValue, f.countEntityPKsErr
}

func (f *fixedStorer) GetEntityPublicKeys(
	entityID string, kt api.KeyType,
) ([]*api.PublicKeyDetail, error) {
	return f.getEntityPKs, f.getEntityPKsErr
}

func (f *fixedStorer) AddPublicKeys(pkds []*api.PublicKeyDetail) error {
	return f.addErr
}

func (f *fixedStorer) GetPublicKeys(pks [][]byte) ([]*api.PublicKeyDetail, error) {
	return f.getPKDs, f.getErr
}

func (f *fixedStorer) Close() error {
	return nil
}
