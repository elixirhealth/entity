package server

import (
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logStorage            = "storage"
	logEntityID           = "entity_id"
	logNewEntity          = "new_entity"
	logEntityType         = "entity_type"
	logDBUrl              = "db_url"
	logQuery              = "query"
	logLimit              = "limit"
	logNFound             = "n_found"
	logKeyType            = "key_type"
	logNKeys              = "n_keys"
	logOfEntityID         = "of_entity_id"
	logRequersterEntityID = "requester_entity_id"
	logNPublicKeys        = "n_public_keys"
	logErr                = "err"
)

func logPutEntityRq(rq *api.PutEntityRequest) []zapcore.Field {
	if rq.Entity == nil {
		return []zapcore.Field{}
	}
	return []zapcore.Field{
		zap.String(logEntityID, rq.Entity.EntityId),
		zap.Bool(logNewEntity, rq.Entity.EntityId == ""),
		zap.String(logEntityType, rq.Entity.Type()),
	}
}

func logSearchEntityRq(rq *api.SearchEntityRequest) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logQuery, rq.Query),
		zap.Uint32(logLimit, rq.Limit),
	}
}

func logPutEntityRp(rq *api.PutEntityRequest, rp *api.PutEntityResponse, new bool) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, rp.EntityId),
		zap.Bool(logNewEntity, new),
		zap.String(logEntityType, rq.Entity.Type()),
	}
}

func logGetEntityRp(rp *api.GetEntityResponse) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, rp.Entity.EntityId),
		zap.String(logEntityType, rp.Entity.Type()),
	}
}

func logSearchEntityRp(rq *api.SearchEntityRequest, rp *api.SearchEntityResponse) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logQuery, rq.Query),
		zap.Uint32(logLimit, rq.Limit),
		zap.Int(logNFound, len(rp.Entities)),
	}
}

func logAddPublicKeysRq(rq *api.AddPublicKeysRequest) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, rq.EntityId),
		zap.Stringer(logKeyType, rq.KeyType),
		zap.Int(logNKeys, len(rq.PublicKeys)),
	}
}

func logGetPublicKeysRq(rq *api.GetPublicKeysRequest) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, rq.EntityId),
		zap.Stringer(logKeyType, rq.KeyType),
	}
}

func logGetPublicKeysRp(
	rq *api.GetPublicKeysRequest, rp *api.GetPublicKeysResponse,
) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, rq.EntityId),
		zap.Stringer(logKeyType, rq.KeyType),
		zap.Int(logNKeys, len(rp.PublicKeys)),
	}
}

func logSamplePublicKeysRq(rq *api.SamplePublicKeysRequest) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logOfEntityID, rq.OfEntityId),
		zap.String(logRequersterEntityID, rq.RequesterEntityId),
		zap.Uint32(logNPublicKeys, rq.NPublicKeys),
	}
}

func logSamplePublicKeysRp(
	rq *api.SamplePublicKeysRequest, rp *api.SamplePublicKeysResponse,
) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logOfEntityID, rq.OfEntityId),
		zap.String(logRequersterEntityID, rq.RequesterEntityId),
		zap.Int(logNPublicKeys, len(rp.PublicKeyDetails)),
	}
}
