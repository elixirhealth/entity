// +build acceptance

package acceptance

import (
	"context"
	"encoding/hex"
	"math/rand"
	"net"
	"testing"
	"time"

	errors2 "github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/entity/pkg/server/storage/postgres/migrations"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/mattes/migrate/source/go-bindata"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type parameters struct {
	nEntities    uint
	nPuts        uint
	nGets        uint
	nSearches    uint
	nKeys        uint
	nKeyTypeKeys uint
	updateRatio  float32
	searchRatio  float32
	searchLimit  uint32
	timeout      time.Duration
	logLevel     zapcore.Level
}

type state struct {
	rng              *rand.Rand
	dbURL            string
	entityServers    []*server.Entity
	entityClients    []api.EntityClient
	tearDownPostgres func() error

	entities          []*api.EntityDetail
	entityAuthorKeys  map[string][][]byte
	entityReaderKeys  map[string][][]byte
	authorKeyEntities map[string]string
	readerKeyEntities map[string]string
}

func TestAcceptance(t *testing.T) {
	params := &parameters{
		nEntities:    4,
		nPuts:        64,
		nGets:        64,
		nSearches:    16,
		nKeys:        3,
		nKeyTypeKeys: 64,
		updateRatio:  0.25,
		searchRatio:  0.75,
		searchLimit:  api.MaxSearchLimit,
		timeout:      3 * time.Second,
		logLevel:     zapcore.InfoLevel,
	}
	st := setUp(t, params)

	testPutNewEntities(t, params, st)

	testPutUpdatedEntities(t, params, st)

	testGetEntities(t, params, st)

	testSearchEntities(t, params, st)

	testAdd(t, params, st)

	testGet(t, params, st)

	testGetDetails(t, params, st)

	testSample(t, params, st)

	tearDown(t, st)
}

func testPutNewEntities(t *testing.T, params *parameters, st *state) {
	st.entities = make([]*api.EntityDetail, params.nPuts)

	for i := range st.entities {
		st.entities[i] = CreateTestEntity(st.rng)

		rq := &api.PutEntityRequest{Entity: st.entities[i]}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().PutEntity(ctx, rq)
		cancel()
		assert.Nil(t, err)
		st.entities[i].EntityId = rp.EntityId
	}
}

func testPutUpdatedEntities(t *testing.T, params *parameters, st *state) {
	for i, e := range st.entities {
		if st.rng.Float32() > params.updateRatio {
			continue
		}
		UpdateTestEntity(e)

		rq := &api.PutEntityRequest{Entity: st.entities[i]}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().PutEntity(ctx, rq)
		cancel()
		assert.Nil(t, err)
		assert.Equal(t, e.EntityId, rp.EntityId)
	}
}

func testGetEntities(t *testing.T, params *parameters, st *state) {
	for _, e := range st.entities {
		rq := &api.GetEntityRequest{EntityId: e.EntityId}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().GetEntity(ctx, rq)
		cancel()
		assert.Nil(t, err)
		assert.Equal(t, e, rp.Entity)
	}
}

func testSearchEntities(t *testing.T, params *parameters, st *state) {
	for _, e := range st.entities {
		if st.rng.Float32() > params.searchRatio {
			continue
		}

		rq := &api.SearchEntityRequest{
			Query: GetTestSearchQueryFromEntity(st.rng, e),
			Limit: params.searchLimit,
		}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().SearchEntity(ctx, rq)
		cancel()
		assert.Nil(t, err)
		assert.True(t, len(rp.Entities) > 0)

		// should find entity in results
		found := false
		for _, re := range rp.Entities {
			if re.EntityId == e.EntityId {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func testAdd(t *testing.T, params *parameters, st *state) {
	for c := uint(0); c < params.nEntities; c++ {
		entityID, authorKeys, readerKeys := CreateTestEntityKeys(st.rng, c, params.nKeyTypeKeys)
		st.entityAuthorKeys[entityID] = authorKeys
		st.entityReaderKeys[entityID] = readerKeys
		for i := range authorKeys {
			st.authorKeyEntities[hex.EncodeToString(authorKeys[i])] = entityID
			st.readerKeyEntities[hex.EncodeToString(readerKeys[i])] = entityID
		}

		rq := &api.AddPublicKeysRequest{
			EntityId:   entityID,
			KeyType:    api.KeyType_AUTHOR,
			PublicKeys: authorKeys,
		}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		_, err := st.randClient().AddPublicKeys(ctx, rq)
		cancel()
		assert.Nil(t, err)

		rq = &api.AddPublicKeysRequest{
			EntityId:   entityID,
			KeyType:    api.KeyType_READER,
			PublicKeys: readerKeys,
		}
		ctx, cancel = context.WithTimeout(context.Background(), params.timeout)
		_, err = st.randClient().AddPublicKeys(ctx, rq)
		cancel()
		assert.Nil(t, err)
	}
}

func testGet(t *testing.T, params *parameters, st *state) {
	for c := uint(0); c < params.nEntities; c++ {
		entityID := GetTestEntityID(c % 4)
		rq := &api.GetPublicKeysRequest{
			EntityId: entityID,
			KeyType:  api.KeyType_READER,
		}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().GetPublicKeys(ctx, rq)
		cancel()
		assert.Nil(t, err)
		assert.Equal(t, len(st.entityReaderKeys[entityID]), len(rp.PublicKeys))
		assert.Equal(t, getPKSet(st.entityReaderKeys[entityID]), getPKSet(rp.PublicKeys))
	}
}

func getPKSet(pks [][]byte) map[string]struct{} {
	pkSet := make(map[string]struct{})
	for _, pk := range pks {
		pkSet[hex.EncodeToString(pk)] = struct{}{}
	}
	return pkSet
}

func testGetDetails(t *testing.T, params *parameters, st *state) {
	for c := uint(0); c < params.nGets; c++ {
		entityID := GetTestEntityID(c % 4)
		// get one random author key, and one random reader key
		nAuthorKeys := len(st.entityAuthorKeys[entityID])
		authorKey := st.entityAuthorKeys[entityID][st.rng.Intn(nAuthorKeys)]
		nReaderKeys := len(st.entityReaderKeys[entityID])
		readerKey := st.entityReaderKeys[entityID][st.rng.Intn(nReaderKeys)]
		authorEntityID := st.authorKeyEntities[hex.EncodeToString(authorKey)]
		readerEntityID := st.readerKeyEntities[hex.EncodeToString(readerKey)]

		rq := &api.GetPublicKeyDetailsRequest{
			PublicKeys: [][]byte{authorKey, readerKey},
		}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().GetPublicKeyDetails(ctx, rq)
		cancel()
		assert.Nil(t, err)
		assert.Equal(t, authorEntityID, rp.PublicKeyDetails[0].EntityId)
		assert.Equal(t, readerEntityID, rp.PublicKeyDetails[1].EntityId)
	}
}

func testSample(t *testing.T, params *parameters, st *state) {
	for c := uint(0); c < params.nEntities; c++ {
		entityID := GetTestEntityID(c)
		rq := &api.SamplePublicKeysRequest{
			OfEntityId:        entityID,
			RequesterEntityId: "some requester",
			NPublicKeys:       1,
		}
		ctx, cancel := context.WithTimeout(context.Background(), params.timeout)
		rp, err := st.randClient().SamplePublicKeys(ctx, rq)
		cancel()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(rp.PublicKeyDetails))
		if len(rp.PublicKeyDetails) == 1 {
			pkHex := hex.EncodeToString(rp.PublicKeyDetails[0].PublicKey)
			assert.Equal(t, entityID, st.readerKeyEntities[pkHex])
		}
	}
}

func setUp(t *testing.T, params *parameters) *state {
	dbURL, cleanup, err := bstorage.StartTestPostgres()
	if err != nil {
		t.Fatal(err)
	}
	st := &state{
		rng:              rand.New(rand.NewSource(0)),
		dbURL:            dbURL,
		tearDownPostgres: cleanup,

		entityAuthorKeys:  make(map[string][][]byte),
		entityReaderKeys:  make(map[string][][]byte),
		authorKeyEntities: make(map[string]string),
		readerKeyEntities: make(map[string]string),
	}
	createAndStartEntities(params, st)
	return st
}

func createAndStartEntities(params *parameters, st *state) {
	configs, addrs := newEntityConfigs(params, st)
	catalogs := make([]*server.Entity, params.nEntities)
	entityClients := make([]api.EntityClient, params.nEntities)
	up := make(chan *server.Entity, 1)

	for i := uint(0); i < params.nEntities; i++ {
		go func() {
			err := server.Start(configs[i], up)
			errors2.MaybePanic(err)
		}()

		// wait for server to come up
		catalogs[i] = <-up

		// set up client to it
		conn, err := grpc.Dial(addrs[i].String(), grpc.WithInsecure())
		errors2.MaybePanic(err)
		entityClients[i] = api.NewEntityClient(conn)
	}

	st.entityServers = catalogs
	st.entityClients = entityClients
}

func newEntityConfigs(params *parameters, st *state) ([]*server.Config, []*net.TCPAddr) {
	startPort := uint(10100)
	configs := make([]*server.Config, params.nEntities)
	addrs := make([]*net.TCPAddr, params.nEntities)

	storageParams := storage.NewDefaultParameters()
	storageParams.Type = bstorage.Postgres

	for i := uint(0); i < params.nEntities; i++ {
		serverPort, metricsPort := startPort+i*10, startPort+i*10+1
		configs[i] = server.NewDefaultConfig().
			WithStorage(storageParams).
			WithDBUrl(st.dbURL)
		configs[i].WithServerPort(uint(serverPort)).
			WithMetricsPort(uint(metricsPort)).
			WithLogLevel(params.logLevel)
		addrs[i] = &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	}
	return configs, addrs
}

func tearDown(t *testing.T, st *state) {
	for _, d := range st.entityServers {
		d.StopServer()
	}
	logger := &bstorage.ZapLogger{Logger: logging.NewDevInfoLogger()}
	m := bstorage.NewBindataMigrator(
		st.dbURL,
		bindata.Resource(migrations.AssetNames(), migrations.Asset),
		logger,
	)
	err := m.Down()
	assert.Nil(t, err)

	err = st.tearDownPostgres()
	assert.Nil(t, err)
}

func (st *state) randClient() api.EntityClient {
	return st.entityClients[st.rng.Int31n(int32(len(st.entityClients)))]
}
