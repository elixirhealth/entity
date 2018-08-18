package entityapi

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"

	"github.com/elixirhealth/service-base/pkg/util"
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

	// MaxSamplePublicKeysSize is the maximum number of public keys that an entity can sample
	// from another entity.
	MaxSamplePublicKeysSize = 8
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

	// ErrEmptyPublicKeys indicates when a list of public keys is nil or zero length.
	ErrEmptyPublicKeys = errors.New("empty public keys list")

	// ErrDupPublicKeys indicates when a list of public keys or public key details has
	// duplicate public keys.
	ErrDupPublicKeys = errors.New("duplicate public keys in list")

	// ErrEmptyPublicKeyDetail indicates when a public key detail value is nil.
	ErrEmptyPublicKeyDetail = errors.New("empty public key detail value")

	// ErrEmptyPublicKey indicates when a public key is nil or zero length.
	ErrEmptyPublicKey = errors.New("empty public key field")

	// ErrEmptyEntityID indicates when the entity ID of a public key detail value is missing.
	ErrEmptyEntityID = errors.New("empty entity ID field")

	// ErrEmptyNPublicKeys indicates when the number of public keys is zero in a
	// SamplePublicKeys request.
	ErrEmptyNPublicKeys = errors.New("missing number of public keys")

	// ErrNPublicKeysTooLarge indicates when the number of public keys in a sample request is
	// larger than the maximum value.
	ErrNPublicKeysTooLarge = fmt.Errorf("number of public keys larger than maximum value %d",
		MaxSamplePublicKeysSize)

	// ErrNoSuchPublicKey indicates when details for a requested public key do not exist.
	ErrNoSuchPublicKey = errors.New("no details found for given public key")

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

// ValidateAddPublicKeysRequest checks that the request has the entity ID and public keys present.
func ValidateAddPublicKeysRequest(rq *AddPublicKeysRequest) error {
	if rq.EntityId == "" {
		return ErrEmptyEntityID
	}
	if err := ValidatePublicKeys(rq.PublicKeys); err != nil {
		return err
	}
	return nil
}

// ValidateGetPublicKeysRequest checks that the entity ID field is not empty.
func ValidateGetPublicKeysRequest(rq *GetPublicKeysRequest) error {
	if rq.EntityId == "" {
		return ErrEmptyEntityID
	}
	return nil
}

// ValidateGetPublicKeyDetailsRequest checks that the request has the public keys present.
func ValidateGetPublicKeyDetailsRequest(rq *GetPublicKeyDetailsRequest) error {
	return ValidatePublicKeys(rq.PublicKeys)
}

// ValidateSamplePublicKeysRequest checks that the request has the entity IDs and number of public
// keys present.
func ValidateSamplePublicKeysRequest(rq *SamplePublicKeysRequest) error {
	if rq.OfEntityId == "" {
		return ErrEmptyEntityID
	}
	if rq.NPublicKeys == 0 {
		return ErrEmptyNPublicKeys
	}
	if rq.NPublicKeys > MaxSamplePublicKeysSize {
		return ErrNPublicKeysTooLarge
	}
	if rq.RequesterEntityId == "" {
		return ErrEmptyEntityID
	}
	return nil
}

// ValidatePublicKeyDetails checks that the list of public key details isn't empty, has no dups,
// and has valid public key detail elements.
func ValidatePublicKeyDetails(pkds []*PublicKeyDetail) error {
	if len(pkds) == 0 {
		return ErrEmptyPublicKeys
	}
	pks := map[string]struct{}{}
	for _, pkd := range pkds {
		if err := ValidatePublicKeyDetail(pkd); err != nil {
			return err
		}
		pkHex := hex.EncodeToString(pkd.PublicKey)
		if _, in := pks[pkHex]; in {
			return ErrDupPublicKeys
		}
		pks[pkHex] = struct{}{}
	}
	return nil
}

// ValidatePublicKeyDetail checks that a public key detail is not empty and has all fields
// populated.
func ValidatePublicKeyDetail(pkd *PublicKeyDetail) error {
	if pkd == nil {
		return ErrEmptyPublicKeyDetail
	}
	if err := ValidatePublicKey(pkd.PublicKey); err != nil {
		return err
	}
	if pkd.EntityId == "" {
		return ErrEmptyEntityID
	}
	return nil
}

// ValidatePublicKeys checks that a list of public keys is not empty, has no dups, and has
// non-empty elements.
func ValidatePublicKeys(pks [][]byte) error {
	if len(pks) == 0 {
		return ErrEmptyPublicKeys
	}
	pkSet := map[string]struct{}{}
	for _, pk := range pks {
		if err := ValidatePublicKey(pk); err != nil {
			return err
		}
		pkHex := hex.EncodeToString(pk)
		if _, in := pkSet[pkHex]; in {
			return ErrDupPublicKeys
		}
		pkSet[pkHex] = struct{}{}
	}
	return nil
}

// ValidatePublicKey checks that a public key is not nil or empty.
func ValidatePublicKey(pk []byte) error {
	if len(pk) == 0 {
		return ErrEmptyPublicKey
	}
	return nil
}

// NewTestPublicKeyDetail creates a random *PublicKeyDetail for use in testing.
func NewTestPublicKeyDetail(rng *rand.Rand) *PublicKeyDetail {
	return &PublicKeyDetail{
		PublicKey: util.RandBytes(rng, 33),
		EntityId:  fmt.Sprintf("EntityID-%d", rng.Intn(4)),
		KeyType:   KeyType(rng.Intn(2)),
	}
}

// NewTestPublicKeyDetails creates a list of random *PublicKeyDetails for use in testing.
func NewTestPublicKeyDetails(rng *rand.Rand, n int) []*PublicKeyDetail {
	pkds := make([]*PublicKeyDetail, n)
	for i := range pkds {
		pkds[i] = NewTestPublicKeyDetail(rng)
	}
	return pkds
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
