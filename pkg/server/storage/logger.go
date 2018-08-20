package storage

import (
	"go.uber.org/zap/zapcore"
)

const (
	logStorageType  = "storage_type"
	logQueryTimeout = "query_timeout"
	logSimilarities = "similarities"
	logSimilarity   = "similarity"
	logEntityID     = "entity_id"
)

// MarshalLogObject writes the parameters to the given object encoder.
func (p *Parameters) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddString(logStorageType, p.Type.String())
	oe.AddDuration(logQueryTimeout, p.Timeout)
	return nil
}
