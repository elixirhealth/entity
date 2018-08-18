package entityapi

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntity_Type(t *testing.T) {
	cases := []struct {
		e        *EntityDetail
		expected string
	}{
		{e: NewTestPatient(0, true), expected: "PATIENT"},
		{e: NewTestOffice(0, true), expected: "OFFICE"},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, c.e.Type())
	}
}

func TestEntity_Name(t *testing.T) {
	cases := []struct {
		e        *EntityDetail
		expected string
	}{
		{e: NewTestPatient(0, true), expected: "First Name 0 Last Name 0"},
		{e: NewTestOffice(0, true), expected: "Office Name 0"},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, c.e.Name())
	}
}

func TestValidatePutEntityRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *PutEntityRequest
		expected error
	}{
		"ok": {
			rq: &PutEntityRequest{
				Entity: NewTestPatient(0, false),
			},
			expected: nil,
		},
		"invalid entity": {
			rq: &PutEntityRequest{
				Entity: &EntityDetail{},
			},
			expected: ErrMissingTypeAttributes,
		},
	}

	for desc, c := range cases {
		err := ValidatePutEntityRequest(c.rq)
		assert.Equal(t, c.expected, err, desc)
	}
}

func TestValidateGetEntityRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *GetEntityRequest
		expected error
	}{
		"ok": {
			rq:       &GetEntityRequest{EntityId: "some entity ID"},
			expected: nil,
		},
		"missing entity ID": {
			rq:       &GetEntityRequest{},
			expected: ErrGetMissingEntityID,
		},
	}

	for desc, c := range cases {
		err := ValidateGetEntityRequest(c.rq)
		assert.Equal(t, c.expected, err, desc)
	}
}

func TestValidateEntity(t *testing.T) {
	cases := map[string]struct {
		e        *EntityDetail
		expected error
	}{
		"ok": {
			e:        NewTestPatient(0, false),
			expected: nil,
		},
		"missing type attributes": {
			e:        &EntityDetail{},
			expected: ErrMissingTypeAttributes,
		},
		"patient missing last name": {
			e: NewPatient("", &PatientAttributes{
				FirstName: "First Name",
				Birthdate: &Date{Year: 2006, Month: 1, Day: 2},
			}),
			expected: ErrPatientMissingLastName,
		},
		"patient missing first name": {
			e: NewPatient("", &PatientAttributes{
				LastName:  "Last Name",
				Birthdate: &Date{Year: 2006, Month: 1, Day: 2},
			}),
			expected: ErrPatientMissingFirstName,
		},
		"patient missing birthdate": {
			e: NewPatient("", &PatientAttributes{
				LastName:  "Last Name",
				FirstName: "First Name",
			}),
			expected: ErrPatientMissingBirthdate,
		},
		"office missing name": {
			e:        NewOffice("", &OfficeAttributes{}),
			expected: ErrOfficeMissingName,
		},
	}

	for desc, c := range cases {
		err := ValidateEntity(c.e)
		assert.Equal(t, c.expected, err, desc)
	}
}

func TestValidateSearchEntityRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *SearchEntityRequest
		expected error
	}{
		"ok": {
			rq: &SearchEntityRequest{
				Query: strings.Repeat("A", 4),
				Limit: 1,
			},
			expected: nil,
		},
		"query too short": {
			rq: &SearchEntityRequest{
				Query: strings.Repeat("A", 3),
				Limit: 1,
			},
			expected: ErrSearchQueryTooShort,
		},
		"query too long": {
			rq: &SearchEntityRequest{
				Query: strings.Repeat("A", 33),
				Limit: 1,
			},
			expected: ErrSearchQueryTooLong,
		},
		"limit too small": {
			rq: &SearchEntityRequest{
				Query: strings.Repeat("A", 4),
				Limit: 0,
			},
			expected: ErrSearchLimitTooSmall,
		},
		"limit too large": {
			rq: &SearchEntityRequest{
				Query: strings.Repeat("A", 4),
				Limit: 9,
			},
			expected: ErrSearchLimitTooLarge,
		},
	}
	for desc, c := range cases {
		err := ValidateSearchEntityRequest(c.rq)
		assert.Equal(t, c.expected, err, desc)
	}
}

func TestDate_ISO8601(t *testing.T) {
	cases := []struct {
		d        *Date
		expected string
	}{
		{d: &Date{Year: 2006, Month: 1, Day: 2}, expected: "2006-01-02"},
		{d: &Date{Year: 2006, Month: 11, Day: 2}, expected: "2006-11-02"},
		{d: &Date{Year: 2006, Month: 11, Day: 12}, expected: "2006-11-12"},
	}
	for _, c := range cases {
		assert.Equal(t, c.expected, c.d.ISO8601())
	}
}

func TestValidateAddPublicKeysRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *AddPublicKeysRequest
		expected error
	}{
		"ok": {
			rq: &AddPublicKeysRequest{
				EntityId:   "some entity ID",
				PublicKeys: [][]byte{{1, 2, 3}},
			},
			expected: nil,
		},
		"missing entity ID": {
			rq: &AddPublicKeysRequest{
				PublicKeys: [][]byte{{1, 2, 3}},
			},
			expected: ErrEmptyEntityID,
		},
		"missing public keys": {
			rq: &AddPublicKeysRequest{
				EntityId: "some entity ID",
			},
			expected: ErrEmptyPublicKeys,
		},
	}
	for _, c := range cases {
		err := ValidateAddPublicKeysRequest(c.rq)
		assert.Equal(t, c.expected, err)
	}
}

func TestValidateGetPublicKeysRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *GetPublicKeysRequest
		expected error
	}{
		"ok": {
			rq: &GetPublicKeysRequest{
				EntityId: "some entity ID",
			},
			expected: nil,
		},
		"missing entity ID": {
			rq:       &GetPublicKeysRequest{},
			expected: ErrEmptyEntityID,
		},
	}
	for _, c := range cases {
		err := ValidateGetPublicKeysRequest(c.rq)
		assert.Equal(t, c.expected, err)
	}
}

func TestValidateGetPublicKeyDetailsRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *GetPublicKeyDetailsRequest
		expected error
	}{
		"ok": {
			rq: &GetPublicKeyDetailsRequest{
				PublicKeys: [][]byte{{1, 2, 3}},
			},
			expected: nil,
		},
		"missing public keys": {
			rq:       &GetPublicKeyDetailsRequest{},
			expected: ErrEmptyPublicKeys,
		},
	}
	for _, c := range cases {
		err := ValidateGetPublicKeyDetailsRequest(c.rq)
		assert.Equal(t, c.expected, err)
	}
}

func TestValidateSamplePublicKeysRequest(t *testing.T) {
	cases := map[string]struct {
		rq       *SamplePublicKeysRequest
		expected error
	}{
		"ok": {
			rq: &SamplePublicKeysRequest{
				OfEntityId:        "some entity ID",
				NPublicKeys:       4,
				RequesterEntityId: "another entity ID",
			},
			expected: nil,
		},
		"missing OfEntityID": {
			rq: &SamplePublicKeysRequest{
				NPublicKeys:       4,
				RequesterEntityId: "another entity ID",
			},
			expected: ErrEmptyEntityID,
		},
		"missing NPublicKeys": {
			rq: &SamplePublicKeysRequest{
				OfEntityId:        "some entity ID",
				RequesterEntityId: "another entity ID",
			},
			expected: ErrEmptyNPublicKeys,
		},
		"NPublicKeys too large": {
			rq: &SamplePublicKeysRequest{
				OfEntityId:        "some entity ID",
				NPublicKeys:       16,
				RequesterEntityId: "another entity ID",
			},
			expected: ErrNPublicKeysTooLarge,
		},
		"missing RequesterEntity": {
			rq: &SamplePublicKeysRequest{
				OfEntityId:  "some entity ID",
				NPublicKeys: 4,
			},
			expected: ErrEmptyEntityID,
		},
	}
	for desc, c := range cases {
		err := ValidateSamplePublicKeysRequest(c.rq)
		assert.Equal(t, c.expected, err, desc)
	}
}

func TestValidatePublicKeyDetails(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	okPKD := NewTestPublicKeyDetail(rng)
	cases := map[string]struct {
		pkds     []*PublicKeyDetail
		expected error
	}{
		"ok": {
			pkds:     []*PublicKeyDetail{okPKD},
			expected: nil,
		},
		"nil value": {
			pkds:     nil,
			expected: ErrEmptyPublicKeys,
		},
		"zero-len value": {
			pkds:     []*PublicKeyDetail{},
			expected: ErrEmptyPublicKeys,
		},
		"pkd missing required fields": {
			pkds:     []*PublicKeyDetail{{}},
			expected: ErrEmptyPublicKey,
		},
		"duplicate pkd": {
			pkds:     []*PublicKeyDetail{okPKD, okPKD},
			expected: ErrDupPublicKeys,
		},
	}
	for _, c := range cases {
		err := ValidatePublicKeyDetails(c.pkds)
		assert.Equal(t, c.expected, err)
	}
}

func TestValidatePublicKeyDetail(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	okPKD := NewTestPublicKeyDetail(rng)
	cases := map[string]struct {
		pkd      *PublicKeyDetail
		expected error
	}{
		"ok": {
			pkd:      okPKD,
			expected: nil,
		},
		"nil value": {
			pkd:      nil,
			expected: ErrEmptyPublicKeyDetail,
		},
		"empty public key": {
			pkd:      &PublicKeyDetail{},
			expected: ErrEmptyPublicKey,
		},
		"missing entity ID": {
			pkd: &PublicKeyDetail{
				PublicKey: []byte{1, 2, 3},
				EntityId:  "",
			},
			expected: ErrEmptyEntityID,
		},
	}
	for _, c := range cases {
		err := ValidatePublicKeyDetail(c.pkd)
		assert.Equal(t, c.expected, err)
	}
}

func TestValidatePublicKeys(t *testing.T) {
	cases := map[string]struct {
		pks      [][]byte
		expected error
	}{
		"ok": {
			pks:      [][]byte{{1, 2, 3}},
			expected: nil,
		},
		"nil value": {
			pks:      nil,
			expected: ErrEmptyPublicKeys,
		},
		"zero-len value": {
			pks:      [][]byte{},
			expected: ErrEmptyPublicKeys,
		},
		"empty pub key": {
			pks:      [][]byte{{}},
			expected: ErrEmptyPublicKey,
		},
		"duplicate pub keys": {
			pks:      [][]byte{{1, 2, 3}, {1, 2, 3}},
			expected: ErrDupPublicKeys,
		},
	}
	for _, c := range cases {
		err := ValidatePublicKeys(c.pks)
		assert.Equal(t, c.expected, err)
	}
}

func TestValidatePublicKey(t *testing.T) {
	cases := map[string]struct {
		pk       []byte
		expected error
	}{
		"ok": {
			pk:       []byte{1, 2, 3},
			expected: nil,
		},
		"nil value": {
			pk:       nil,
			expected: ErrEmptyPublicKey,
		},
		"zero-len value": {
			pk:       []byte{},
			expected: ErrEmptyPublicKey,
		},
	}
	for _, c := range cases {
		err := ValidatePublicKey(c.pk)
		assert.Equal(t, c.expected, err)
	}
}
