package storage

import (
	"math"
	"time"

	errors2 "github.com/drausin/libri/libri/common/errors"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage/id"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

var (
	// ErrMissingEntity indicates when an entity is requested with an ID that does not exist.
	ErrMissingEntity = errors.New("no entity with given ID")

	// ErrDupGenEntityID indicates when a newly generated entity ID already exists.
	ErrDupGenEntityID = errors.New("duplicate entity ID generated")

	// ErrUnknownEntityType indicates when the entity type is unknown (usually used in default
	// case of switch statement).
	ErrUnknownEntityType = errors.New("unknown entity type")

	// ErrMaxBatchSizeExceeded indicates when the number of public keys an in an add or get
	// request ot the storer exceeds the maximum size.
	ErrMaxBatchSizeExceeded = errors.New("number of public keys in request exceeeds max " +
		"batch size")

	// DefaultMaxBatchSize is the maximum size of a batch of public keys.
	DefaultMaxBatchSize = uint(64)

	// MaxEntityKeyTypeKeys indicates the maximum number of public keys an entity can have for
	// a given key type.
	MaxEntityKeyTypeKeys = 256

	// DefaultStorageType is the default storage type.
	DefaultStorageType = bstorage.Memory

	// DefaultTimeout is the default timeout for DB queries.
	DefaultTimeout = 2 * time.Second
)

// Storer stores and retrieves entities.
type Storer interface {
	// PutEntity inserts a new or updates an existing entity (based on E.EntityId) and returns
	// the entity ID.
	PutEntity(e *api.EntityDetail) (string, error)

	// GetEntity retrives the entity with the given entityID.
	GetEntity(entityID string) (*api.EntityDetail, error)

	// SearchEntity finds {{ limiit }} entities matching the given query, ordered most similar
	// to least.
	SearchEntity(query string, limit uint) ([]*api.EntityDetail, error)

	// AddPublicKeys stores a list of public keys details.
	AddPublicKeys(pkds []*api.PublicKeyDetail) error

	// GetPublicKeyDetails returns a public key detail for each public key.
	GetPublicKeys(pks [][]byte) ([]*api.PublicKeyDetail, error)

	// GetEntityPublicKeys returns the public keys of a given type associated with an entity.
	GetEntityPublicKeys(entityID string, kt api.KeyType) ([]*api.PublicKeyDetail, error)

	// CountEntityPublicKeys counts the number of public keys of a given type for an entity.
	CountEntityPublicKeys(entityID string, kt api.KeyType) (int, error)

	// Close handles any necessary cleanup.
	Close() error
}

// Parameters defines the parameters of the Storer.
type Parameters struct {
	Type         bstorage.Type
	Timeout      time.Duration
	MaxBatchSize uint
}

// NewDefaultParameters returns a *Parameters object with default values.
func NewDefaultParameters() *Parameters {
	return &Parameters{
		Type:         DefaultStorageType,
		Timeout:      DefaultTimeout,
		MaxBatchSize: DefaultMaxBatchSize,
	}
}

type searcherSimilarities map[string]float32

func (ss searcherSimilarities) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for searcher, sim := range ss {
		enc.AddFloat32(searcher, sim)
	}
	return nil
}

// EntitySim contains an *api.EntityDetail and its Similarities to the query for a number of
// different Searches
type EntitySim struct {
	E                  *api.EntityDetail
	Similarities       searcherSimilarities
	similaritySuffStat float32
}

// NewEntitySim creates a new *EntitySim for the given *Entity.
func NewEntitySim(e *api.EntityDetail) *EntitySim {
	return &EntitySim{
		E:            e,
		Similarities: make(map[string]float32),
	}
}

// MarshalLogObject writes the EntitySim to the given ObjectEncoder.
func (e *EntitySim) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString(logEntityID, e.E.EntityId)
	enc.AddFloat32(logSimilarity, e.Similarity())
	err := enc.AddObject(logSimilarities, e.Similarities)
	errors2.MaybePanic(err) // should never happen
	return nil
}

// Add adds a new [0, 1] similarity score for the given search name.
func (e *EntitySim) Add(search string, similarity float32) {
	e.Similarities[search] = similarity
	// L-2 suff stat is sum of squares
	e.similaritySuffStat += similarity * similarity
}

// Similarity returns the combined similarity over all the searches.
func (e *EntitySim) Similarity() float32 {
	return float32(math.Sqrt(float64(e.similaritySuffStat)))
}

// EntitySims is a min-heap of entity Similarities
type EntitySims []*EntitySim

// MarshalLogArray writes the ArrayEncoder to the given ArrayEncoder.
func (ess EntitySims) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, es := range ess {
		err := enc.AppendObject(es)
		errors2.MaybePanic(err) // should never happen
	}
	return nil
}

// Len returns the number of entity sims.
func (ess EntitySims) Len() int {
	return len(ess)
}

// Less returns whether entity sim i has a similarity less than that of j.
func (ess EntitySims) Less(i, j int) bool {
	return ess[i].Similarity() < ess[j].Similarity()
}

// Swap swaps the entity sim i and j.
func (ess EntitySims) Swap(i, j int) {
	ess[i], ess[j] = ess[j], ess[i]
}

// Push adds the given EntitySim to the heap.
func (ess *EntitySims) Push(x interface{}) {
	*ess = append(*ess, x.(*EntitySim))
}

// Pop removes the EntitySim from the root of the heap.
func (ess *EntitySims) Pop() interface{} {
	old := *ess
	n := len(old)
	x := old[n-1]
	*ess = old[0 : n-1]
	return x
}

// Peak returns the EntitySim from the root of the heap.
func (ess EntitySims) Peak() *EntitySim {
	return ess[0]
}

// MaybeAddEntityID adds a newly generated ID from idGen if e.EntityId is missing.
func MaybeAddEntityID(e *api.EntityDetail, idGen id.Generator) (added bool, err error) {
	if e.EntityId != "" {
		return false, nil
	}
	idPrefix := GetEntityType(e).IDPrefix()
	entityID, err := idGen.Generate(idPrefix)
	if err != nil {
		return false, err
	}
	e.EntityId = entityID
	return true, nil
}
