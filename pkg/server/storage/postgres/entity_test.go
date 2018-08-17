package postgres

import (
	"testing"
	"time"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/stretchr/testify/assert"
)

func TestPreparePatientScan(t *testing.T) {
	e1 := api.NewTestPatient(0, true)
	p1 := e1.Attributes.(*api.EntityDetail_Patient).Patient

	cols, dests, scan := prepPatientScan(0)
	assert.Equal(t, len(cols), len(dests))

	// simulate row.Scan(dest...)
	dests[0] = &e1.EntityId
	dests[1] = &p1.LastName
	dests[2] = &p1.FirstName
	dests[3] = &p1.MiddleName
	dests[4] = &p1.Suffix
	birthdateTime, err := time.Parse("2006-01-02", p1.Birthdate.ISO8601())
	assert.Nil(t, err)
	dests[5] = &birthdateTime

	e2 := scan()
	assert.Equal(t, e1, e2)
}

func TestPrepareOfficeScan(t *testing.T) {
	e1 := &api.EntityDetail{
		EntityId: "some entity ID",
		Attributes: &api.EntityDetail_Office{
			Office: &api.OfficeAttributes{
				Name: "Name 1",
			},
		},
	}
	f1 := e1.Attributes.(*api.EntityDetail_Office).Office

	cols, dests, create := prepOfficeScan(0)
	assert.Equal(t, len(cols), len(dests))

	// simulate row.Scan(dest...)
	dests[0] = &e1.EntityId
	dests[1] = &f1.Name

	e2 := create()
	assert.Equal(t, e1, e2)
}
