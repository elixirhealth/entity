package memory

import (
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logQuery       = "query"
	logEntityID    = "entity_id"
	logInsert      = "insert"
	logUpdate      = "update"
	logLimit       = "limit"
	logResults     = "results"
	logNPublicKeys = "n_public_keys"
	logKeyType     = "key_type"
)

func logPutResult(entityID string, insert bool) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Bool(logInsert, insert),
		zap.Bool(logUpdate, !insert),
	}
}

func logSearchRanked(query string, limit uint, ess storage.EntitySims) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logQuery, query),
		zap.Uint(logLimit, limit),
		zap.Array(logResults, ess),
	}
}

func logGetEntityPubKeys(entityID string, pkds []*api.PublicKeyDetail) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Int(logNPublicKeys, len(pkds)),
	}
}
func logCountEntityPubKeys(entityID string, kt api.KeyType) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Stringer(logKeyType, kt),
	}
}
