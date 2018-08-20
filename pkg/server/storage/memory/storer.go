package memory

import (
	"container/heap"
	"encoding/hex"
	"sort"
	"strings"
	"sync"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/entity/pkg/server/storage/id"
	"go.uber.org/zap"
)

type storer struct {
	params   *storage.Parameters
	idGen    id.Generator
	entities map[string]*api.EntityDetail
	pkds     map[string]*api.PublicKeyDetail
	mu       sync.Mutex
	logger   *zap.Logger
}

// New creates a new Storer backed by an in-memory map with the given id.Generator, params, and
// logger.
func New(idGen id.Generator, params *storage.Parameters, logger *zap.Logger) storage.Storer {
	return &storer{
		params:   params,
		idGen:    idGen,
		entities: make(map[string]*api.EntityDetail),
		pkds:     make(map[string]*api.PublicKeyDetail),
		logger:   logger,
	}
}

func (s *storer) PutEntity(e *api.EntityDetail) (string, error) {
	insert := true
	if e.EntityId != "" {
		insert = false
		if err := s.idGen.Check(e.EntityId); err != nil {
			return "", err
		}
	}
	if err := api.ValidateEntity(e); err != nil {
		return "", err
	}
	if _, err := storage.MaybeAddEntityID(e, s.idGen); err != nil {
		return "", err
	}
	s.mu.Lock()
	if _, in := s.entities[e.EntityId]; in && insert {
		return "", storage.ErrDupGenEntityID
	}
	s.entities[e.EntityId] = e
	s.mu.Unlock()
	s.logger.Debug("successfully entities entity", logPutResult(e.EntityId, insert)...)
	return e.EntityId, nil
}

func (s *storer) GetEntity(entityID string) (*api.EntityDetail, error) {
	if err := s.idGen.Check(entityID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	e, in := s.entities[entityID]
	s.mu.Unlock()
	if !in {
		return nil, storage.ErrMissingEntity
	}
	s.logger.Debug("successfully found entity", zap.String(logEntityID, entityID))
	return e, nil
}

func (s *storer) SearchEntity(query string, limit uint) ([]*api.EntityDetail, error) {
	if err := api.ValidateSearchQuery(query, uint32(limit)); err != nil {
		return nil, err
	}
	s.mu.Lock()
	ess := &storage.EntitySims{}
	heap.Init(ess)

	// just loop through all entities once
	for _, e := range s.entities {
		if matches, searcher, sim := checkMatchesQuery(e, query); matches {
			es := storage.NewEntitySim(e)
			es.Add(searcher, sim)
			if ess.Len() < int(limit) || es.Similarity() > ess.Peak().Similarity() {
				heap.Push(ess, es)
			}
			if ess.Len() > int(limit) {
				heap.Pop(ess)
			}
		}
	}
	s.mu.Unlock()

	sort.Sort(sort.Reverse(ess)) // sort descending
	result := make([]*api.EntityDetail, 0, limit)
	s.logger.Debug("ranked search results", logSearchRanked(query, limit, *ess)...)
	for _, es := range *ess {
		result = append(result, es.E)
	}
	return result, nil
}

func (s *storer) AddPublicKeys(pkds []*api.PublicKeyDetail) error {
	if err := api.ValidatePublicKeyDetails(pkds); err != nil {
		return err
	}
	for _, pkd := range pkds {
		pkHex := hex.EncodeToString(pkd.PublicKey)
		s.mu.Lock()
		s.pkds[pkHex] = pkd
		s.mu.Unlock()
	}
	s.logger.Debug("added public keys to storage", zap.Int(logNPublicKeys, len(pkds)))
	return nil
}

func (s *storer) GetPublicKeys(pks [][]byte) ([]*api.PublicKeyDetail, error) {
	if err := api.ValidatePublicKeys(pks); err != nil {
		return nil, err
	}
	pkds := make([]*api.PublicKeyDetail, 0, len(pks))
	for _, pk := range pks {
		pkHex := hex.EncodeToString(pk)
		s.mu.Lock()
		pkd, in := s.pkds[pkHex]
		if !in {
			s.mu.Unlock()
			return nil, api.ErrNoSuchPublicKey
		}
		pkds = append(pkds, pkd)
		s.mu.Unlock()
	}
	s.logger.Debug("got public keys from storage", zap.Int(logNPublicKeys, len(pkds)))
	return pkds, nil
}

func (s *storer) GetEntityPublicKeys(
	entityID string, kt api.KeyType,
) ([]*api.PublicKeyDetail, error) {
	if entityID == "" {
		return nil, api.ErrEmptyEntityID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pkds := make([]*api.PublicKeyDetail, 0, storage.MaxEntityKeyTypeKeys)
	for _, pkd := range s.pkds {
		if pkd.EntityId == entityID && pkd.KeyType == kt {
			pkds = append(pkds, pkd)
		}
	}
	s.logger.Debug("found public keys for entity", logGetEntityPubKeys(entityID, pkds)...)
	return pkds, nil
}

func (s *storer) CountEntityPublicKeys(entityID string, kt api.KeyType) (int, error) {
	if entityID == "" {
		return 0, api.ErrEmptyEntityID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := 0
	for _, pkd := range s.pkds {
		if pkd.EntityId == entityID && pkd.KeyType == kt {
			c++
		}
	}
	s.logger.Debug("counted public keys for entity", logCountEntityPubKeys(entityID, kt)...)
	return c, nil
}

func (s *storer) Close() error {
	return nil
}

// checkMatchesQuery checks whether an entity matches a given query under various searcher,
// returning the first match it finds, if any
func checkMatchesQuery(
	e *api.EntityDetail, query string) (matches bool, searcher string, sim float32,
) {
	if matches, sim = matchesUpper(query, e.EntityId); matches {
		return true, "EntityID", sim
	}
	switch ta := e.Attributes.(type) {
	case *api.EntityDetail_Patient:
		p := ta.Patient
		if matches, sim = matchesUpper(query, p.LastName, p.FirstName); matches {
			return true, "PatientName", sim
		}
		if matches, sim = matchesUpper(query, p.FirstName, p.LastName); matches {
			return true, "PatientName", sim
		}
	case *api.EntityDetail_Office:
		f := ta.Office
		if matches, sim = matchesUpper(query, f.Name); matches {
			return true, "OfficeName", sim
		}
	}
	return false, "", 0
}

func matchesUpper(query string, vals ...string) (matches bool, sim float32) {
	concatVals := strings.ToUpper(strings.Join(vals, " "))
	if strings.Contains(concatVals, strings.ToUpper(query)) {
		return true, float32(len(query)) / float32(len(concatVals))
	}
	return false, 0
}
