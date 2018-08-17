package postgres

import (
	"strings"
	"time"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
)

const (
	entitySchema  = "entity"
	entityIDCol   = "entity_id"
	similarityCol = "sim"

	// patient attribute indexedValue
	lastNameCol   = "last_name"
	firstNameCol  = "first_name"
	middleNameCol = "middle_name"
	suffixCol     = "suffix"
	birthdateCol  = "birthdate"

	// office attribute indexedValue
	nameCol = "name"
)

func fullTableName(et storage.EntityType) string {
	return entitySchema + "." + strings.ToLower(et.String())
}

// getPutStmtValues returns a map of column name -> value for the given entity for use in an
// INSERT or UPDATE statement
func getPutStmtValues(e *api.EntityDetail) map[string]interface{} {
	var vals map[string]interface{}
	switch ta := e.Attributes.(type) {
	case *api.EntityDetail_Patient:
		vals = getPutPatientStmtValues(ta.Patient)
	case *api.EntityDetail_Office:
		vals = getPutOfficeStmtValues(ta.Office)
	default:
		panic(storage.ErrUnknownEntityType)
	}
	vals[entityIDCol] = e.EntityId
	return vals
}

// prepEntityScan returns the table columns, destination slice, and an entity creation function for
// use in an entity SELECT statement
func prepEntityScan(
	et storage.EntityType, nExtraDest int,
) (cols []string, dest []interface{}, create func() *api.EntityDetail) {
	switch et {
	case storage.Patient:
		return prepPatientScan(nExtraDest)
	case storage.Office:
		return prepOfficeScan(nExtraDest)
	default:
		panic(storage.ErrUnknownEntityType)
	}
}

func prepPatientScan(nExtraDests int) ([]string, []interface{}, func() *api.EntityDetail) {
	p := &api.PatientAttributes{}
	e := &api.EntityDetail{Attributes: &api.EntityDetail_Patient{Patient: p}}
	var birthdateTime time.Time
	cds := []*colDest{
		{entityIDCol, &e.EntityId},
		{lastNameCol, &p.LastName},
		{firstNameCol, &p.FirstName},
		{middleNameCol, &p.MiddleName},
		{suffixCol, &p.Suffix},
		{birthdateCol, &birthdateTime},
	}
	cols, dests := splitColDests(cds, nExtraDests)
	return cols, dests, func() *api.EntityDetail {
		e.EntityId = *dests[0].(*string)
		p.LastName = *dests[1].(*string)
		p.FirstName = *dests[2].(*string)
		p.MiddleName = *dests[3].(*string)
		p.Suffix = *dests[4].(*string)
		birthdateTime := *dests[5].(*time.Time)
		p.Birthdate = &api.Date{
			Year:  uint32(birthdateTime.Year()),
			Month: uint32(birthdateTime.Month()),
			Day:   uint32(birthdateTime.Day()),
		}
		return e
	}
}

func prepOfficeScan(nExtraDests int) ([]string, []interface{}, func() *api.EntityDetail) {
	f := &api.OfficeAttributes{}
	e := &api.EntityDetail{Attributes: &api.EntityDetail_Office{Office: f}}
	cds := []*colDest{
		{entityIDCol, &e.EntityId},
		{nameCol, &f.Name},
	}
	cols, dests := splitColDests(cds, nExtraDests)
	return cols, dests, func() *api.EntityDetail {
		e.EntityId = *dests[0].(*string)
		f.Name = *dests[1].(*string)
		return e
	}
}

func getPutPatientStmtValues(p *api.PatientAttributes) map[string]interface{} {
	return map[string]interface{}{
		lastNameCol:   p.LastName,
		firstNameCol:  p.FirstName,
		middleNameCol: p.MiddleName,
		suffixCol:     p.Suffix,
		birthdateCol:  p.Birthdate.ISO8601(),
	}
}

func getPutOfficeStmtValues(f *api.OfficeAttributes) map[string]interface{} {
	return map[string]interface{}{
		nameCol: f.Name,
	}
}

type colDest struct {
	col  string
	dest interface{}
}

func splitColDests(cds []*colDest, nExtraDest int) ([]string, []interface{}) {
	dests := make([]interface{}, len(cds), len(cds)+nExtraDest)
	cols := make([]string, len(cds))
	for i, colDest := range cds {
		cols[i] = colDest.col
		dests[i] = colDest.dest
	}
	return cols, dests
}
