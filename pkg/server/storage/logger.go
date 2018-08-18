package storage

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/errors"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logStorageType  = "storage_type"
	logQueryTimeout = "query_timeout"
	logSimilarities = "similarities"
	logSimilarity   = "similarity"
	logNPublicKeys  = "n_public_keys"
	logEntityID     = "entity_id"
	logKeyType      = "key_type"
	logSQL          = "sql"
	logArgs         = "args"
	logCount        = "count"
)

// MarshalLogObject writes the parameters to the given object encoder.
func (p *Parameters) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddString(logStorageType, p.Type.String())
	oe.AddDuration(logQueryTimeout, p.Timeout)
	return nil
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
