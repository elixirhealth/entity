package server

import (
	"errors"
	"testing"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var okEntity = api.NewTestPatient(0, false)

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

func TestDirectory_PutEntity_ok(t *testing.T) {
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

func TestDirectory_PutEntity_err(t *testing.T) {
	baseServer := server.NewBaseServer(server.NewDefaultBaseConfig())
	cases := map[string]struct {
		d  *Entity
		rq *api.PutEntityRequest
	}{
		"invalid request": {
			d: &Entity{
				BaseServer: baseServer,
			},
			rq: &api.PutEntityRequest{},
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
		},
	}

	for desc, c := range cases {
		rp, err := c.d.PutEntity(context.Background(), c.rq)
		assert.NotNil(t, err, desc)
		assert.Nil(t, rp, desc)
	}
}

func TestDirectory_GetEntity_ok(t *testing.T) {
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

func TestDirectory_GetEntity_err(t *testing.T) {
	baseServer := server.NewBaseServer(server.NewDefaultBaseConfig())
	cases := map[string]struct {
		d  *Entity
		rq *api.GetEntityRequest
	}{
		"invalid request": {
			d: &Entity{
				BaseServer: baseServer,
			},
			rq: &api.GetEntityRequest{},
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
		},
	}

	for desc, c := range cases {
		rp, err := c.d.GetEntity(context.Background(), c.rq)
		assert.NotNil(t, err, desc)
		assert.Nil(t, rp, desc)
	}
}

func TestDirectory_SearchEntity_ok(t *testing.T) {
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

func TestDirectory_SearchEntity_err(t *testing.T) {
	baseServer := server.NewBaseServer(server.NewDefaultBaseConfig())
	cases := map[string]struct {
		d  *Entity
		rq *api.SearchEntityRequest
	}{
		"invalid request": {
			d: &Entity{
				BaseServer: baseServer,
			},
			rq: &api.SearchEntityRequest{},
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
			},
		},
	}

	for desc, c := range cases {
		rp, err := c.d.SearchEntity(context.Background(), c.rq)
		assert.NotNil(t, err, desc)
		assert.Nil(t, rp, desc)
	}
}

type fixedStorer struct {
	putEntityID    string
	putErr         error
	getEntity      *api.EntityDetail
	getErr         error
	searchEntities []*api.EntityDetail
	searchErr      error
	closeErr       error
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

func (f *fixedStorer) Close() error {
	return f.closeErr
}
