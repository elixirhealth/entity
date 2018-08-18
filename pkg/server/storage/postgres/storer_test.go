package postgres

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/logging"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/entity/pkg/server/storage/id"
	"github.com/elixirhealth/entity/pkg/server/storage/postgres/migrations"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/mattes/migrate/source/go-bindata"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	setUpPostgresTest func(t *testing.T) (dbURL string, tearDown func() error)

	errTest = errors.New("test error")
)

func TestMain(m *testing.M) {
	dbURL, cleanup, err := bstorage.StartTestPostgres()
	if err != nil {
		if err2 := cleanup(); err2 != nil {
			log.Fatal("test postgres cleanup error: " + err2.Error())
		}
		log.Fatal("test postgres start error: " + err.Error())
	}
	as := bindata.Resource(migrations.AssetNames(), migrations.Asset)
	logger := &bstorage.LogLogger{}
	setUpPostgresTest = func(t *testing.T) (string, func() error) {
		m := bstorage.NewBindataMigrator(dbURL, as, logger)
		if err := m.Up(); err != nil {
			t.Fatal(err)
		}
		return dbURL, m.Down
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := cleanup(); err != nil {
		log.Fatal(err.Error())
	}

	os.Exit(code)
}

func TestNewPostgres_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	params := storage.NewDefaultParameters()
	cases := map[string]struct {
		dbURL  string
		idGen  id.Generator
		params *storage.Parameters
	}{
		"empty DB URL": {
			idGen:  idGen,
			params: params,
		},
		"wrong bstorage type": {
			dbURL: "some DB URL",
			idGen: idGen,
			params: &storage.Parameters{
				Type: bstorage.Unspecified,
			},
		},
	}

	for desc, c := range cases {
		s, err := New(c.dbURL, c.idGen, c.params, lg)
		assert.NotNil(t, err, desc)
		assert.Nil(t, s, desc)
	}
}

func TestStorer_PutGetEntity_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	lg := logging.NewDevLogger(zapcore.DebugLevel)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	s, err := New(dbURL, idGen, params, lg)
	assert.Nil(t, err)
	assert.NotNil(t, s)

	cases := map[storage.EntityType]struct {
		original *api.EntityDetail
		updated  *api.EntityDetail
	}{
		storage.Patient: {
			original: api.NewPatient("", &api.PatientAttributes{
				LastName:  "Last Name 1",
				FirstName: "First Name 1",
				Birthdate: &api.Date{Year: 2006, Month: 1, Day: 2},
			}),
			updated: api.NewPatient("", &api.PatientAttributes{
				LastName:  "Last Name 2",
				FirstName: "First Name 1",
				Birthdate: &api.Date{Year: 2006, Month: 1, Day: 2},
			}),
		},

		storage.Office: {
			original: api.NewOffice("", &api.OfficeAttributes{
				Name: "Name 1",
			}),
			updated: api.NewOffice("", &api.OfficeAttributes{
				Name: "Name 2",
			}),
		},
	}
	assert.Equal(t, storage.NEntityTypes, len(cases))

	for et, c := range cases {
		assert.Equal(t, et, storage.GetEntityType(c.original), et.String())
		assert.NotEqual(t, c.original, c.updated)

		entityID, err := s.PutEntity(c.original)
		assert.Nil(t, err, et.String())
		assert.Equal(t, entityID, c.original.EntityId, et.String())
	}

	for et, c := range cases {
		entityID := c.original.EntityId
		gottenOriginal, err := s.GetEntity(entityID)
		assert.Nil(t, err, et.String())
		assert.Equal(t, c.original, gottenOriginal)

		c.updated.EntityId = c.original.EntityId
		entityID, err = s.PutEntity(c.updated)
		assert.Nil(t, err)
		assert.Equal(t, entityID, c.updated.EntityId)

		gottenUpdated, err := s.GetEntity(entityID)
		assert.Nil(t, err)
		assert.Equal(t, c.updated, gottenUpdated)
	}
}

func TestStorer_GetEntity_err(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop()
	s, err := New(dbURL, idGen, params, lg)
	assert.Nil(t, err)
	assert.NotNil(t, s)

	// bad ID
	e, err := s.GetEntity("bad ID")
	assert.NotNil(t, err)
	assert.Nil(t, e)

	// missing ID
	missingID, err := idGen.Generate(storage.Patient.IDPrefix())
	assert.Nil(t, err)
	e, err = s.GetEntity(missingID)
	assert.Equal(t, storage.ErrMissingEntity, err)
	assert.Nil(t, e)
}

func TestStorer_PutEntity_err(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	okIDGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	okID, err := okIDGen.Generate(storage.Patient.IDPrefix())
	assert.Nil(t, err)
	okEntity := api.NewTestPatient(0, false)

	cases := map[string]struct {
		s *storer
		e *api.EntityDetail
	}{
		"bad entity ID": {
			s: &storer{
				idGen: okIDGen,
			},
			e: &api.EntityDetail{EntityId: "bad ID"},
		},
		"bad entity": {
			s: &storer{
				idGen: okIDGen,
			},
			e: &api.EntityDetail{},
		},
		"ID gen error": {
			s: &storer{
				idGen: &fixedIDGen{generateErr: errors.New("some Generate error")},
			},
			e: okEntity,
		},
	}

	for desc, c := range cases {
		entityID, err2 := c.s.PutEntity(c.e)
		assert.NotNil(t, err2, desc)
		assert.Empty(t, entityID, desc)
	}

	// two puts with same gen'd ID
	s, err := New(dbURL, &fixedIDGen{generateID: okID}, params, lg)
	assert.Nil(t, err)
	okEntity.EntityId = ""
	_, err = s.PutEntity(okEntity)
	assert.Nil(t, err)
	okEntity.EntityId = ""
	_, err = s.PutEntity(okEntity)
	assert.Equal(t, storage.ErrDupGenEntityID, err)
}

func TestStorer_SearchEntity_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	lg := logging.NewDevLogger(zapcore.DebugLevel)
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	s, err := New(dbURL, idGen, params, lg)
	assert.Nil(t, err)
	assert.NotNil(t, s)

	es := []*api.EntityDetail{
		api.NewTestPatient(1, false),
		api.NewTestPatient(2, false),
		api.NewTestPatient(3, false),
		api.NewTestPatient(4, false),
		api.NewTestOffice(1, false),
		api.NewTestOffice(2, false),
		api.NewTestOffice(3, false),
		api.NewTestOffice(4, false),
	}
	entityIDs := make([]string, len(es))
	for i, e := range es {
		entityID, err2 := s.PutEntity(e)
		assert.Nil(t, err2)
		entityIDs[i] = entityID
	}

	limit := uint(3)

	query := "ice name 1" // query unanchored substring with diff case
	found, err := s.SearchEntity(query, limit)
	assert.Nil(t, err)
	assert.Equal(t, limit, uint(len(found)))

	// check that first result is the office with the name that matches the query
	f, ok := found[0].Attributes.(*api.EntityDetail_Office)
	assert.True(t, ok)
	assert.True(t, strings.Contains(strings.ToUpper(f.Office.Name), strings.ToUpper(query)))

	// check that second and third results are also offices
	_, ok = found[1].Attributes.(*api.EntityDetail_Office)
	assert.True(t, ok)
	_, ok = found[2].Attributes.(*api.EntityDetail_Office)
	assert.True(t, ok)

	query = strings.ToLower(entityIDs[1][:4]) // 2nd patient's first 4 chars of entityID diff case
	found, err = s.SearchEntity(query, limit)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(found))

	// check that first result is the patient with an entityID that matches the query
	_, ok = found[0].Attributes.(*api.EntityDetail_Patient)
	assert.True(t, ok)
	assert.True(t, strings.HasPrefix(found[0].EntityId, strings.ToUpper(query)))
}

func TestStorer_SearchEntity_err(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	lg := zap.NewNop()
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	okStorer, err := New(dbURL, idGen, params, lg)
	assert.Nil(t, err)
	assert.NotNil(t, okStorer)

	okQuery := "some query"
	okLimit := uint(3)

	cases := map[string]struct {
		getStorer func() storage.Storer
		query     string
		limit     uint
		expected  error
	}{
		"query too short": {
			getStorer: func() storage.Storer { return okStorer },
			query:     "A",
			limit:     okLimit,
			expected:  api.ErrSearchQueryTooShort,
		},
		"query too long": {
			getStorer: func() storage.Storer { return okStorer },
			query:     strings.Repeat("A", 33),
			limit:     okLimit,
			expected:  api.ErrSearchQueryTooLong,
		},
		"limit too small": {
			getStorer: func() storage.Storer { return okStorer },
			query:     okQuery,
			limit:     0,
			expected:  api.ErrSearchLimitTooSmall,
		},
		"limit too large": {
			getStorer: func() storage.Storer { return okStorer },
			query:     okQuery,
			limit:     9,
			expected:  api.ErrSearchLimitTooLarge,
		},
		"unexpected query error": {
			getStorer: func() storage.Storer {
				s, err := New(dbURL, idGen, params, lg)
				assert.Nil(t, err)
				assert.NotNil(t, s)
				s.(*storer).qr = &fixedQuerier{selectErr: errTest}
				return s

			},
			query:    okQuery,
			limit:    okLimit,
			expected: errTest,
		},
		"unexpected merge error": {
			getStorer: func() storage.Storer {
				s, err := New(dbURL, idGen, params, lg)
				assert.Nil(t, err)
				assert.NotNil(t, s)
				s.(*storer).qr = &fixedQuerier{}
				s.(*storer).newSRM = func() searchResultMerger {
					return &fixedSearchResultsMerger{
						mergeErr: errTest,
					}
				}
				return s
			},
			query:    okQuery,
			limit:    okLimit,
			expected: errTest,
		},
		"queryRows error": {
			getStorer: func() storage.Storer {
				s, err := New(dbURL, idGen, params, lg)
				assert.Nil(t, err)
				assert.NotNil(t, s)
				s.(*storer).qr = &fixedQuerier{
					selectResult: &fixedOfficeRows{
						errErr: errTest,
					},
				}
				s.(*storer).newSRM = func() searchResultMerger {
					return &fixedSearchResultsMerger{}
				}
				return s
			},
			query:    okQuery,
			limit:    okLimit,
			expected: errTest,
		},
		"queryRows close error": {
			getStorer: func() storage.Storer {
				s, err := New(dbURL, idGen, params, lg)
				assert.Nil(t, err)
				assert.NotNil(t, s)
				s.(*storer).qr = &fixedQuerier{
					selectResult: &fixedOfficeRows{
						closeErr: errTest,
					},
				}
				s.(*storer).newSRM = func() searchResultMerger {
					return &fixedSearchResultsMerger{}
				}
				return s
			},
			query:    okQuery,
			limit:    okLimit,
			expected: errTest,
		},
	}

	for desc, c := range cases {
		s := c.getStorer()
		result, err := s.SearchEntity(c.query, c.limit)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, result, desc)
	}
}

func TestStorer_AddGetPublicKeys_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	pkds1 := api.NewTestPublicKeyDetails(rng, 8)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := logging.NewDevLogger(zap.DebugLevel)
	s, err := New(dbURL, idGen, params, lg)
	assert.Nil(t, err)

	err = s.AddPublicKeys(pkds1)
	assert.Nil(t, err)

	pubKeys := make([][]byte, len(pkds1))
	for i, pkd := range pkds1 {
		pubKeys[i] = pkd.PublicKey
	}
	pkds2, err := s.GetPublicKeys(pubKeys)
	assert.Nil(t, err)
	assert.Equal(t, len(pkds1), len(pkds2))
}

func TestStorer_AddPublicKeys_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := logging.NewDevLogger(zap.DebugLevel)

	cases := map[string]struct {
		s        *storer
		pkds     []*api.PublicKeyDetail
		expected error
	}{
		"bad PKDs": {
			s:        &storer{params: params},
			pkds:     []*api.PublicKeyDetail{},
			expected: api.ErrEmptyPublicKeys,
		},
		"batch too large": {
			s:        &storer{params: params},
			pkds:     api.NewTestPublicKeyDetails(rng, 128),
			expected: storage.ErrMaxBatchSizeExceeded,
		},
		"insert err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					insertErr: errTest,
				},
			},
			pkds:     api.NewTestPublicKeyDetails(rng, 8),
			expected: errTest,
		},
	}
	for desc, c := range cases {
		err := c.s.AddPublicKeys(c.pkds)
		assert.Equal(t, c.expected, err, desc)
	}
}

func TestStorer_GetPublicKeys_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := logging.NewDevLogger(zap.DebugLevel)
	n := 128
	pubKeys := make([][]byte, n)
	for i, pkd := range api.NewTestPublicKeyDetails(rng, n) {
		pubKeys[i] = pkd.PublicKey
	}

	cases := map[string]struct {
		s        *storer
		pks      [][]byte
		expected error
	}{
		"bad PKDs": {
			s:        &storer{params: params},
			pks:      [][]byte{},
			expected: api.ErrEmptyPublicKeys,
		},
		"batch too large": {
			s:        &storer{params: params},
			pks:      pubKeys,
			expected: storage.ErrMaxBatchSizeExceeded,
		},
		"select err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectErr: errTest,
				},
			},
			pks:      pubKeys[:8],
			expected: errTest,
		},
		"rows scan err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectResult: &fixedRowScanner{
						next:    true,
						scanErr: errTest,
					},
				},
			},
			pks:      pubKeys[:8],
			expected: errTest,
		},
		"rows err err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectResult: &fixedRowScanner{
						errErr: errTest,
					},
				},
			},
			pks:      pubKeys[:8],
			expected: errTest,
		},
	}
	for desc, c := range cases {
		pkds, err := c.s.GetPublicKeys(c.pks)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, pkds)
	}
}

func TestStorer_GetCountEntityPublicKeys_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	idGen := id.NewNaiveLuhnGenerator(rng, id.DefaultLength)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := logging.NewDevLogger(zap.DebugLevel)
	pkds1 := api.NewTestPublicKeyDetails(rng, 64)

	s, err := New(dbURL, idGen, params, lg)
	assert.Nil(t, err)

	err = s.AddPublicKeys(pkds1)
	assert.Nil(t, err)

	entityID := pkds1[0].EntityId
	pkds2, err := s.GetEntityPublicKeys(entityID, api.KeyType_READER)
	assert.Nil(t, err)
	assert.True(t, len(pkds2) > 1)
	for _, pkd := range pkds2 {
		assert.Equal(t, entityID, pkd.EntityId)
		assert.Equal(t, api.KeyType_READER, pkd.KeyType)
		assert.NotEmpty(t, pkd.PublicKey)
	}

	n, err := s.CountEntityPublicKeys(entityID, api.KeyType_READER)
	assert.Nil(t, err)
	assert.Equal(t, len(pkds2), n)
}

func TestStorer_GetEntityPublicKeys_err(t *testing.T) {
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := logging.NewDevLogger(zap.DebugLevel)
	entityID := "some entity ID"

	cases := map[string]struct {
		s        *storer
		entityID string
		expected error
	}{
		"bad entityID": {
			s:        &storer{params: params},
			entityID: "",
			expected: api.ErrEmptyEntityID,
		},
		"select err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectErr: errTest,
				},
			},
			entityID: entityID,
			expected: errTest,
		},
		"rows scan err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectResult: &fixedRowScanner{
						next:    true,
						scanErr: errTest,
					},
				},
			},
			entityID: entityID,
			expected: errTest,
		},
		"rows err err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectResult: &fixedRowScanner{
						errErr: errTest,
					},
				},
			},
			entityID: entityID,
			expected: errTest,
		},
	}
	for desc, c := range cases {
		pkds, err := c.s.GetEntityPublicKeys(c.entityID, api.KeyType_READER)
		assert.Equal(t, c.expected, err, desc)
		assert.Nil(t, pkds)
	}
}

func TestStorer_CountEntityPublicKeys_err(t *testing.T) {
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := logging.NewDevLogger(zap.DebugLevel)
	entityID := "some entity ID"

	cases := map[string]struct {
		s        *storer
		entityID string
		expected error
	}{
		"bad entityID": {
			s:        &storer{params: params},
			entityID: "",
			expected: api.ErrEmptyEntityID,
		},
		"select row err": {
			s: &storer{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					selectRowResult: &fixedRowScanner{
						scanErr: errTest,
					},
				},
			},
			entityID: entityID,
			expected: errTest,
		},
	}
	for desc, c := range cases {
		pkds, err := c.s.CountEntityPublicKeys(c.entityID, api.KeyType_READER)
		assert.Equal(t, c.expected, err, desc)
		assert.Zero(t, pkds)
	}
}

type fixedIDGen struct {
	checkErr    error
	generateID  string
	generateErr error
}

func (f *fixedIDGen) Check(id string) error {
	return f.checkErr
}

func (f *fixedIDGen) Generate(prefix string) (string, error) {
	return f.generateID, f.generateErr
}

type fixedQuerier struct {
	selectResult    bstorage.QueryRows
	selectErr       error
	selectRowResult sq.RowScanner
	insertResult    sql.Result
	insertErr       error
}

func (f *fixedQuerier) SelectQueryContext(
	ctx context.Context, b sq.SelectBuilder,
) (bstorage.QueryRows, error) {
	return f.selectResult, f.selectErr
}

func (f *fixedQuerier) SelectQueryRowContext(
	ctx context.Context, b sq.SelectBuilder,
) sq.RowScanner {
	return f.selectRowResult
}

func (f *fixedQuerier) InsertExecContext(
	ctx context.Context, b sq.InsertBuilder,
) (sql.Result, error) {
	return f.insertResult, f.insertErr
}

func (f *fixedQuerier) UpdateExecContext(
	ctx context.Context, b sq.UpdateBuilder,
) (sql.Result, error) {
	panic("implement me")
}

func (f *fixedQuerier) DeleteExecContext(
	ctx context.Context, b sq.DeleteBuilder,
) (sql.Result, error) {
	panic("implement me")
}

type fixedSearchResultsMerger struct {
	mergeN        int
	mergeErr      error
	topEntitySims storage.EntitySims
}

func (srm *fixedSearchResultsMerger) merge(
	rows bstorage.QueryRows, searchName string, et storage.EntityType,
) (int, error) {
	return srm.mergeN, srm.mergeErr
}

func (srm *fixedSearchResultsMerger) top(n uint) storage.EntitySims {
	return srm.topEntitySims
}

type fixedRowScanner struct {
	next    bool
	scanErr error
	errErr  error
}

func (f *fixedRowScanner) Next() bool {
	return f.next
}

func (f *fixedRowScanner) Close() error {
	panic("implement me")
}

func (f *fixedRowScanner) Err() error {
	return f.errErr
}

func (f *fixedRowScanner) Scan(...interface{}) error {
	return f.scanErr
}
