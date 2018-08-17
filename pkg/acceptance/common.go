package acceptance

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/Pallinder/go-randomdata"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
)

// CreateTestEntity creates a random test entity.
func CreateTestEntity(rng *rand.Rand) *api.EntityDetail {
	et := storage.EntityType(rng.Int31n(storage.NEntityTypes))

	switch et {
	case storage.Patient:
		return api.NewPatient("", &api.PatientAttributes{
			LastName:   randomdata.LastName(),
			FirstName:  randomdata.FirstName(randomdata.RandomGender),
			MiddleName: randomdata.FirstName(randomdata.RandomGender),
			Birthdate: &api.Date{
				Day:   uint32(rng.Int31n(28)) + 1,
				Month: uint32(rng.Int31n(12)) + 1,
				Year:  1950 + uint32(rng.Int31n(60)),
			},
		})
	case storage.Office:
		return api.NewOffice("", &api.OfficeAttributes{
			Name: randomdata.SillyName(),
		})
	default:
		panic(fmt.Sprintf("no test entity creation defined for entity type %s",
			et.String()))
	}
}

// UpdateTestEntity updates a field of the existing entity with a new (random) value.
func UpdateTestEntity(e *api.EntityDetail) {
	switch ta := e.Attributes.(type) {
	case *api.EntityDetail_Patient:
		ta.Patient.LastName = randomdata.LastName()
	case *api.EntityDetail_Office:
		ta.Office.Name = randomdata.SillyName()
	default:
		panic("no test entity creation defined for entity type")
	}
}

// GetTestSearchQueryFromEntity returns a search query string that should return the given entity.
func GetTestSearchQueryFromEntity(rng *rand.Rand, e *api.EntityDetail) string {
	switch e.Attributes.(type) {
	case *api.EntityDetail_Patient:
		return getTestSearchQueryFromPatient(rng, e)
	case *api.EntityDetail_Office:
		return getTestSearchQueryFromOffice(rng, e)
	default:
		panic("no test entity creation defined for entity type")
	}
}

func getTestSearchQueryFromPatient(rng *rand.Rand, e *api.EntityDetail) string {
	var query string
	p := e.Attributes.(*api.EntityDetail_Patient).Patient
	for len(query) < api.MinSearchQueryLen {
		switch rng.Int31n(6) {
		case 0:
			query = e.EntityId
		case 1:
			query = p.LastName
		case 2:
			query = p.FirstName
		case 3:
			query = p.LastName + " " + p.FirstName
		case 4:
			query = p.LastName + ", " + p.FirstName
		case 5:
			query = p.FirstName + " " + p.LastName
		}
	}
	return strings.ToLower(query)
}

func getTestSearchQueryFromOffice(rng *rand.Rand, e *api.EntityDetail) string {
	var query string
	f := e.Attributes.(*api.EntityDetail_Office).Office
	for len(query) < api.MinSearchQueryLen {
		switch rng.Int31n(2) {
		case 0:
			query = e.EntityId
		case 1:
			query = f.Name
		}
	}
	return strings.ToLower(query)
}
