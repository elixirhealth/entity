package postgres

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/errors"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logEntityType  = "entity_type"
	logQuery       = "query"
	logSQL         = "sql"
	logArgs        = "args"
	logEntityID    = "entity_id"
	logInsert      = "insert"
	logUpdate      = "update"
	logSearcher    = "searcher"
	logLimit       = "limit"
	logResults     = "results"
	logNFound      = "n_found"
	logNPublicKeys = "n_public_keys"
	logKeyType     = "key_type"
	logCount       = "count"
)

func logGetSelect(q sq.SelectBuilder, et storage.EntityType, entityID string) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Stringer(logEntityType, et),
		zap.String(logEntityID, entityID),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logSearchSelect(q sq.SelectBuilder, s searcher, query string) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logQuery, query),
		zap.String(logSearcher, s.name()),
		zap.Stringer(logEntityType, s.entityType()),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logSearcherFinished(s searcher, query string, nFound int) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logQuery, query),
		zap.String(logSearcher, s.name()),
		zap.Stringer(logEntityType, s.entityType()),
		zap.Int(logNFound, nFound),
	}
}

func logSearchRanked(query string, limit uint, ess storage.EntitySims) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logQuery, query),
		zap.Uint(logLimit, limit),
		zap.Array(logResults, ess),
	}
}

func logPutInsert(q sq.InsertBuilder, e *api.EntityDetail) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Stringer(logEntityType, storage.GetEntityType(e)),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logPutResult(entityID string, insert bool) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Bool(logInsert, insert),
		zap.Bool(logUpdate, !insert),
	}
}

func logPutUpdate(q sq.UpdateBuilder, e *api.EntityDetail) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Stringer(logEntityType, storage.GetEntityType(e)),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logAddingPublicKeys(q sq.InsertBuilder, pkds []*api.PublicKeyDetail) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Int(logNPublicKeys, len(pkds)),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logAddedPublicKeys(pkds []*api.PublicKeyDetail) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNPublicKeys, len(pkds)),
	}
}

func logGettingPublicKeys(q sq.SelectBuilder, pks [][]byte) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Int(logNPublicKeys, len(pks)),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logGettingEntityPubKeys(q sq.SelectBuilder, entityID string) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logGotEntityPubKeys(entityID string, pkds []*api.PublicKeyDetail) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Int(logNPublicKeys, len(pkds)),
	}
}

func logCountingEntityPubKeys(q sq.SelectBuilder, entityID string, kt api.KeyType) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Stringer(logKeyType, kt),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logCountEntityPubKeys(entityID string, kt api.KeyType, count int) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEntityID, entityID),
		zap.Stringer(logKeyType, kt),
		zap.Int(logCount, count),
	}
}

type queryArgs []interface{}

func (qas queryArgs) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, qa := range qas {
		switch val := qa.(type) {
		case string:
			enc.AppendString(val)
		default:
			if err := enc.AppendReflected(qa); err != nil {
				return err
			}
		}
	}
	return nil
}
