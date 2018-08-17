package entityapi

import (
	"errors"
	"fmt"
)

const (
	// MinSearchQueryLen is the minimum length for an entity search query.
	MinSearchQueryLen = 4

	// MaxSearchQueryLen is the maximum length for an entity search query.
	MaxSearchQueryLen = 32

	// MinSearchLimit is the minimum size for an entity search limit.
	MinSearchLimit = 1

	// MaxSearchLimit is the maximum size for an entity search limit.
	MaxSearchLimit = 8
)

var (
	// ErrPutMissingEntity denotes when a Put request is missing the Entity object.
	ErrPutMissingEntity = errors.New("put request missing entity")

	// ErrGetMissingEntityID denotes when a get request is missing the entity ID.
	ErrGetMissingEntityID = errors.New("get request missing entity ID")

	// ErrMissingTypeAttributes denotes when an entity is missing the expected type_attributes
	// field.
	ErrMissingTypeAttributes = errors.New("entity missing type_attributes")

	// ErrPatientMissingLastName denotes when a patient entity is missing the last name.
	ErrPatientMissingLastName = errors.New("patient missing last name")

	// ErrPatientMissingFirstName denotes when a patient entity is missing the first name.
	ErrPatientMissingFirstName = errors.New("patient missing first name")

	// ErrPatientMissingBirthdate denotes when a patient entity is missing the birthdate.
	ErrPatientMissingBirthdate = errors.New("patient missing birthdate")

	// ErrOfficeMissingName denotes when an office entity is missing the name.
	ErrOfficeMissingName = errors.New("office missing name")

	// ErrSearchQueryTooShort identifies when a search query string is shorter than the minimum
	// length.
	ErrSearchQueryTooShort = fmt.Errorf("search query shorter than min length %d",
		MinSearchQueryLen)

	// ErrSearchQueryTooLong identifies when a search query string is longer than the maximum
	// length.
	ErrSearchQueryTooLong = fmt.Errorf("search query longer than max length %d",
		MaxSearchQueryLen)

	// ErrSearchLimitTooSmall identifies when a search limit is smaller than the minimum value.
	ErrSearchLimitTooSmall = fmt.Errorf("search limit smaller than min length %d",
		MinSearchLimit)

	// ErrSearchLimitTooLarge identifies when a search limit is alarger than the maximum value.
	ErrSearchLimitTooLarge = fmt.Errorf("search limit larger than max length %d",
		MaxSearchLimit)

	errUnknownEntityType = errors.New("unknown entity type")
)

// ValidatePutEntityRequest checks that the PutEntityRequest has the required fields populated.
func ValidatePutEntityRequest(rq *PutEntityRequest) error {
	if rq.Entity == nil {
		return ErrPutMissingEntity
	}
	return ValidateEntity(rq.Entity)
}

// ValidateGetEntityRequest checks that the GetEntityRequest has the required fields populated.
func ValidateGetEntityRequest(rq *GetEntityRequest) error {
	if rq.EntityId == "" {
		return ErrGetMissingEntityID
	}
	return nil
}

// ValidateEntity validates that the entity has the expected fields populated given its type. It
// does not validate that the EntityId is present or of any particular form.
func ValidateEntity(e *EntityDetail) error {
	if e.Attributes == nil {
		return ErrMissingTypeAttributes
	}
	switch ta := e.Attributes.(type) {
	case *EntityDetail_Patient:
		return validatePatient(ta.Patient)
	case *EntityDetail_Office:
		return validateOffice(ta.Office)
	}
	panic(errUnknownEntityType)
}

// ValidateSearchEntityRequest checks that the SearchEntityRequest fields have values within the
// required ranges/sizes.
func ValidateSearchEntityRequest(rq *SearchEntityRequest) error {
	return ValidateSearchQuery(rq.Query, rq.Limit)
}

// ValidateSearchQuery checks that the query and limit have values within the required ranges/sizes.
func ValidateSearchQuery(query string, limit uint32) error {
	if len(query) < MinSearchQueryLen {
		return ErrSearchQueryTooShort
	}
	if len(query) > MaxSearchQueryLen {
		return ErrSearchQueryTooLong
	}
	if limit > MaxSearchLimit {
		return ErrSearchLimitTooLarge
	}
	if limit < MinSearchLimit {
		return ErrSearchLimitTooSmall
	}
	return nil
}

func validatePatient(p *PatientAttributes) error {
	if p.LastName == "" {
		return ErrPatientMissingLastName
	}
	if p.FirstName == "" {
		return ErrPatientMissingFirstName
	}
	if p.Birthdate == nil {
		return ErrPatientMissingBirthdate
	}
	return nil
}

func validateOffice(p *OfficeAttributes) error {
	if p.Name == "" {
		return ErrOfficeMissingName
	}
	return nil
}

// Type returns a string descriptor of the entity type.
func (m *EntityDetail) Type() string {
	switch m.Attributes.(type) {
	case *EntityDetail_Patient:
		return "PATIENT"
	case *EntityDetail_Office:
		return "OFFICE"
	default:
		panic(errUnknownEntityType)
	}
}

// Name return a displayable name for the entity, dependant on type.
func (m *EntityDetail) Name() string {
	switch ta := m.Attributes.(type) {
	case *EntityDetail_Patient:
		return ta.Patient.FirstName + " " + ta.Patient.LastName
	case *EntityDetail_Office:
		return ta.Office.Name
	default:
		panic(errUnknownEntityType)
	}
}

// ISO8601 returns the YYYY-MM-DD ISO 8601 date string.
func (m *Date) ISO8601() string {
	return fmt.Sprintf("%04d-%02d-%02d", m.Year, m.Month, m.Day)
}

// NewPatient returns an *Entity with the given entityID and wrapping the given *Patient.
func NewPatient(entityID string, p *PatientAttributes) *EntityDetail {
	return &EntityDetail{
		EntityId: entityID,
		Attributes: &EntityDetail_Patient{
			Patient: p,
		},
	}
}

// NewOffice returns an *Entity with the given entityID and wrapping the given *Office.
func NewOffice(entityID string, f *OfficeAttributes) *EntityDetail {
	return &EntityDetail{
		EntityId: entityID,
		Attributes: &EntityDetail_Office{
			Office: f,
		},
	}
}

// NewTestPatient returns a new patient entity suitable for use in tests.
func NewTestPatient(i int, addID bool) *EntityDetail {
	entityID := ""
	if addID {
		entityID = fmt.Sprintf("entity %d", i)
	}
	return NewPatient(entityID, &PatientAttributes{
		LastName:   fmt.Sprintf("Last Name %d", i),
		FirstName:  fmt.Sprintf("First Name %d", i),
		MiddleName: fmt.Sprintf("Middle Name %d", i),
		Birthdate:  &Date{Year: 2006, Month: 1, Day: 1 + uint32(i)},
	})
}

// NewTestOffice returns a new office entity suitable for use in tests.
func NewTestOffice(i int, addID bool) *EntityDetail {
	entityID := ""
	if addID {
		entityID = fmt.Sprintf("entity %d", i)
	}
	return NewOffice(entityID, &OfficeAttributes{
		Name: fmt.Sprintf("Office Name %d", i),
	})
}
