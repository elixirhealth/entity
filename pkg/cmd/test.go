package cmd

import (
	"context"
	"encoding/hex"
	"errors"
	"math/rand"
	"time"

	"github.com/drausin/libri/libri/common/logging"
	"github.com/drausin/libri/libri/common/parse"
	"github.com/elixirhealth/entity/pkg/acceptance"
	"github.com/elixirhealth/entity/pkg/entityapi"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/service-base/pkg/cmd"
	"github.com/elixirhealth/service-base/pkg/server"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	timeoutFlag = "timeout"

	logEntityID          = "entity_id"
	logKeyType           = "key_type"
	logNKeys             = "n_keys"
	logExpected          = "expected"
	logActual            = "actual"
	logAuthorKeyShortHex = "author_key_short_hex"
	logReaderKeyShortHex = "reader_key_short_hex"
	logNResults          = "n_results"
)

func testIO() error {
	rng := rand.New(rand.NewSource(0))
	logger := logging.NewDevLogger(logging.GetLogLevel(viper.GetString(logLevelFlag)))
	timeout := time.Duration(viper.GetInt(timeoutFlag) * 1e9)
	nEntities := uint(viper.GetInt(nEntitiesFlag))
	nSearches := uint(viper.GetInt(nSearchesFlag))

	clients, err := getClients()
	if err != nil {
		return err
	}

	// put entities
	entities := make([]*api.EntityDetail, nEntities)
	for i := range entities {
		entities[i] = acceptance.CreateTestEntity(rng)
		client := clients[rng.Int31n(int32(len(clients)))]
		rq := &api.PutEntityRequest{Entity: entities[i]}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rp, err := client.PutEntity(ctx, rq)
		cancel()
		if err != nil {
			logger.Error("entity put failed", zap.Error(err))
			return err
		}
		entities[i].EntityId = rp.EntityId
		logger.Info("entity put succeeded", zap.String(logEntityID, rp.EntityId))
	}

	// search entities
	for c := uint(0); c < nSearches; c++ {
		e := entities[rng.Int31n(int32(nEntities))]
		client := clients[rng.Int31n(int32(len(clients)))]
		rq := &api.SearchEntityRequest{
			Query: acceptance.GetTestSearchQueryFromEntity(rng, e),
			Limit: api.MaxSearchLimit,
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rp, err := client.SearchEntity(ctx, rq)
		cancel()
		if err != nil {
			logger.Error("entity search failed", zap.Error(err))
			return err
		}
		logger.Info("found search results", zap.Int(logNResults, len(rp.Entities)))
	}

	return testKeysIO(clients, logger)
}

func testKeysIO(clients []entityapi.EntityClient, logger *zap.Logger) error {
	rng := rand.New(rand.NewSource(0))
	nEntities := uint(viper.GetInt(nEntitiesFlag))
	timeout := time.Duration(viper.GetInt(timeoutFlag) * 1e9)
	nKeyTypeKeys := uint(64)
	nGets := uint(16)

	entityAuthorKeys := make(map[string][][]byte)
	entityReaderKeys := make(map[string][][]byte)
	authorKeyEntities := make(map[string]string)
	readerKeyEntities := make(map[string]string)

	// create & add keys for each entity
	for c := uint(0); c < nEntities; c++ {
		entityID, authorKeys, readerKeys :=
			acceptance.CreateTestEntityKeys(rng, c, nKeyTypeKeys)
		entityAuthorKeys[entityID] = authorKeys
		entityReaderKeys[entityID] = readerKeys
		for i := range authorKeys {
			authorKeyEntities[hex.EncodeToString(authorKeys[i])] = entityID
			readerKeyEntities[hex.EncodeToString(readerKeys[i])] = entityID
		}

		rq := &api.AddPublicKeysRequest{
			EntityId:   entityID,
			KeyType:    api.KeyType_AUTHOR,
			PublicKeys: authorKeys,
		}
		client := clients[rng.Int31n(int32(len(clients)))]
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, err := client.AddPublicKeys(ctx, rq)
		cancel()
		if err2 := logAddPublicKeysRq(logger, rq, err); err2 != nil {
			return err2
		}

		rq = &api.AddPublicKeysRequest{
			EntityId:   entityID,
			KeyType:    api.KeyType_READER,
			PublicKeys: readerKeys,
		}
		client = clients[rng.Int31n(int32(len(clients)))]
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		_, err = client.AddPublicKeys(ctx, rq)
		cancel()
		if err2 := logAddPublicKeysRq(logger, rq, err); err2 != nil {
			return err
		}
	}

	// get keys
	for c := uint(0); c < nGets; c++ {
		entityID := acceptance.GetTestEntityID(c % 4)
		// get one random author key, and one random reader key
		authorKey := entityAuthorKeys[entityID][rng.Intn(len(entityAuthorKeys))]
		readerKey := entityReaderKeys[entityID][rng.Intn(len(entityReaderKeys))]
		authorEntityID := authorKeyEntities[hex.EncodeToString(authorKey)]
		readerEntityID := readerKeyEntities[hex.EncodeToString(readerKey)]

		rq := &api.GetPublicKeyDetailsRequest{
			PublicKeys: [][]byte{authorKey, readerKey},
		}
		client := clients[rng.Int31n(int32(len(clients)))]
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rp, err := client.GetPublicKeyDetails(ctx, rq)
		cancel()
		err2 := logGetPublicKeyDetailsRp(logger, authorEntityID, readerEntityID, authorKey,
			readerKey, rp, err)
		if err2 != nil {
			return err
		}

	}

	// sample key
	for c := uint(0); c < nEntities; c++ {
		entityID := acceptance.GetTestEntityID(c)
		rq := &api.SamplePublicKeysRequest{
			OfEntityId:        entityID,
			RequesterEntityId: "some requester",
			NPublicKeys:       1,
		}
		client := clients[rng.Int31n(int32(len(clients)))]
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rp, err := client.SamplePublicKeys(ctx, rq)
		cancel()
		err2 := logSamplePublicKeysRp(logger, entityID, readerKeyEntities, rp, err)
		if err2 != nil {
			return err
		}
	}
	return nil
}

func logAddPublicKeysRq(logger *zap.Logger, rq *api.AddPublicKeysRequest, err error) error {
	if err != nil {
		logger.Error("adding public keys failed", zap.Error(err))
		return err
	}
	logger.Info("added public keys",
		zap.String(logEntityID, rq.EntityId),
		zap.Stringer(logKeyType, rq.KeyType),
		zap.Int(logNKeys, len(rq.PublicKeys)),
	)
	return nil
}

func logGetPublicKeyDetailsRp(
	logger *zap.Logger,
	authorEntityID, readerEntityID string,
	authorKey, readerKey []byte,
	rp *api.GetPublicKeyDetailsResponse, err error,
) error {
	if err != nil {
		logger.Error("get public keys failed", zap.Error(err))
		return err
	}
	if authorEntityID != rp.PublicKeyDetails[0].EntityId {
		logger.Error("unexpected entity ID for gotten author key",
			zap.String(logExpected, authorEntityID),
			zap.String(logActual, rp.PublicKeyDetails[0].EntityId),
		)
		return err
	}
	if readerEntityID != rp.PublicKeyDetails[1].EntityId {
		logger.Error("unexpected entity ID for gotten reader key",
			zap.String(logExpected, authorEntityID),
			zap.String(logActual, rp.PublicKeyDetails[0].EntityId),
		)
		return err
	}
	logger.Info("got public key details",
		zap.String(logAuthorKeyShortHex, hex.EncodeToString(authorKey[:8])),
		zap.String(logReaderKeyShortHex, hex.EncodeToString(readerKey[:8])),
	)
	return nil
}

func logSamplePublicKeysRp(
	logger *zap.Logger,
	entityID string,
	readerKeyEntities map[string]string,
	rp *api.SamplePublicKeysResponse,
	err error,
) error {
	if err != nil {
		logger.Error("sample public keys failed", zap.String(logEntityID, entityID))
		return err
	}
	pkHex := hex.EncodeToString(rp.PublicKeyDetails[0].PublicKey)
	if entityID != readerKeyEntities[pkHex] {
		logger.Error("unexpected entityID for sampled key",
			zap.String(logExpected, entityID),
			zap.String(logActual, readerKeyEntities[pkHex]),
		)
		return errors.New("unexpected entityID for sampled key")
	}
	pkShortHex := hex.EncodeToString(rp.PublicKeyDetails[0].PublicKey[:8])
	logger.Info("sampled public key",
		zap.String(logEntityID, entityID),
		zap.String(logReaderKeyShortHex, pkShortHex),
	)
	return nil
}

func getClients() ([]entityapi.EntityClient, error) {
	addrs, err := parse.Addrs(viper.GetStringSlice(cmd.AddressesFlag))
	if err != nil {
		return nil, err
	}
	dialer := server.NewInsecureDialer()
	clients := make([]entityapi.EntityClient, len(addrs))
	for i, addr := range addrs {
		conn, err2 := dialer.Dial(addr.String())
		if err != nil {
			return nil, err2
		}
		clients[i] = entityapi.NewEntityClient(conn)
	}
	return clients, nil
}
