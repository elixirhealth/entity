package storage

import (
	"testing"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/stretchr/testify/assert"
)

func TestEntityType_String(t *testing.T) {
	for i := 0; i < NEntityTypes; i++ {
		et := EntityType(i)
		assert.NotEmpty(t, et.String())
	}
}

func TestEntityType_IDPrefix(t *testing.T) {
	for i := 0; i < NEntityTypes; i++ {
		et := EntityType(i)
		assert.NotEmpty(t, et.IDPrefix())
	}
}

func TestGetEntityType(t *testing.T) {
	cases := map[EntityType]*api.EntityDetail{
		Patient: {Attributes: &api.EntityDetail_Patient{}},
		Office:  {Attributes: &api.EntityDetail_Office{}},
	}
	assert.Equal(t, NEntityTypes, len(cases))
	for et, e := range cases {
		assert.Equal(t, et, GetEntityType(e))
	}
}

func TestGetEntityTypeFromID(t *testing.T) {
	cases := map[EntityType]string{
		Patient: "PAAAAAAA",
		Office:  "FAAAAAAA",
	}
	assert.Equal(t, NEntityTypes, len(cases))
	for et, id := range cases {
		assert.Equal(t, et, GetEntityTypeFromID(id))
	}
}
