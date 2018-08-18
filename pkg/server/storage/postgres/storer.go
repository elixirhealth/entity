package postgres

import (
	"context"
	"database/sql"
	"sync"

	sq "github.com/Masterminds/squirrel"
	errors2 "github.com/drausin/libri/libri/common/errors"
	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/elixirhealth/entity/pkg/server/storage"
	"github.com/elixirhealth/entity/pkg/server/storage/id"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	keySchema            = "key"
	publicKeyDetailTable = "public_key_detail"

	publicKeyCol = "public_key"
	keyTypeCol   = "key_type"

	count                    = "COUNT(*)"
	pqUniqueViolationErrCode = "23505"
)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	fqPublicKeyDetailTable = keySchema + "." + publicKeyDetailTable

	errEmptyDBUrl            = errors.New("empty DB URL")
	errUnexpectedStorageType = errors.New("unexpected storage type")
)

type storer struct {
	params  *storage.Parameters
	idGen   id.Generator
	db      *sql.DB
	dbCache sq.DBProxyContext
	qr      bstorage.Querier
	newSRM  func() searchResultMerger
	logger  *zap.Logger
}

// New creates a new Storer backed by a Postgres DB at the given dbURL and with the
// given id.Generator, params, and logger.
func New(
	dbURL string, idGen id.Generator, params *storage.Parameters, logger *zap.Logger,
) (storage.Storer, error) {
	if dbURL == "" {
		return nil, errEmptyDBUrl
	}
	if params.Type != bstorage.Postgres {
		return nil, errUnexpectedStorageType
	}
	db, err := sql.Open("postgres", dbURL)
	errors2.MaybePanic(err)
	return &storer{
		params:  params,
		idGen:   idGen,
		db:      db,
		dbCache: sq.NewStmtCacher(db),
		qr:      bstorage.NewQuerier(),
		newSRM:  func() searchResultMerger { return newSearchResultMerger() },
		logger:  logger,
	}, nil
}

func (s *storer) PutEntity(e *api.EntityDetail) (string, error) {
	if e.EntityId != "" {
		if err := s.idGen.Check(e.EntityId); err != nil {
			return "", err
		}
	}
	if err := api.ValidateEntity(e); err != nil {
		return "", err
	}
	insert, err := storage.MaybeAddEntityID(e, s.idGen)
	if err != nil {
		return "", err
	}
	fqTbl := fullTableName(storage.GetEntityType(e))
	vals := getPutStmtValues(e)
	ctx, cancel := context.WithTimeout(context.Background(), s.params.Timeout)
	if insert {
		q := psql.RunWith(s.dbCache).
			Insert(fqTbl).
			SetMap(vals)
		s.logger.Debug("inserting entity", logPutInsert(q, e)...)
		_, err = s.qr.InsertExecContext(ctx, q)
	} else {
		q := psql.RunWith(s.dbCache).
			Update(fqTbl).
			SetMap(vals).
			Where(sq.Eq{entityIDCol: e.EntityId})
		s.logger.Debug("updating entity", logPutUpdate(q, e)...)
		_, err = s.qr.UpdateExecContext(ctx, q)
	}
	cancel()
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok {
			if pqErr.Code == pqUniqueViolationErrCode {
				return "", storage.ErrDupGenEntityID
			}
		}
		return "", err
	}
	s.logger.Debug("successfully stored entity", logPutResult(e.EntityId, insert)...)
	return e.EntityId, nil
}

func (s *storer) GetEntity(entityID string) (*api.EntityDetail, error) {
	if err := s.idGen.Check(entityID); err != nil {
		return nil, err
	}
	et := storage.GetEntityTypeFromID(entityID)
	cols, dest, create := prepEntityScan(et, 0)
	q := psql.RunWith(s.dbCache).
		Select(cols...).
		From(fullTableName(et)).
		Where(sq.Eq{entityIDCol: entityID})
	s.logger.Debug("getting entity", logGetSelect(q, et, entityID)...)
	ctx, cancel := context.WithTimeout(context.Background(), s.params.Timeout)
	defer cancel()
	row := s.qr.SelectQueryRowContext(ctx, q)
	if err := row.Scan(dest...); err == sql.ErrNoRows {
		return nil, storage.ErrMissingEntity
	} else if err != nil {
		return nil, err
	}
	s.logger.Debug("successfully found entity", zap.String(logEntityID, entityID))
	return create(), nil
}

func (s *storer) SearchEntity(query string, limit uint) ([]*api.EntityDetail, error) {
	if err := api.ValidateSearchQuery(query, uint32(limit)); err != nil {
		return nil, err
	}
	errs := make(chan error, len(searchers))
	wg1 := new(sync.WaitGroup)
	srm := s.newSRM()
	for _, s1 := range searchers {
		wg1.Add(1)
		go func(s2 searcher, wg2 *sync.WaitGroup) {
			defer wg2.Done()
			entityCols, _, _ := prepEntityScan(s2.entityType(), 0)
			selectCols := append(entityCols, s2.similarity())
			q := psql.RunWith(s.dbCache).
				Select(selectCols...).
				From(fullTableName(s2.entityType())).
				Where(s2.predicate(), s2.preprocQuery(query)).
				OrderBy(similarityCol + " DESC").
				Limit(uint64(limit))
			s.logger.Debug("searching for entity", logSearchSelect(q, s2, query)...)
			ctx, cancel := context.WithTimeout(context.Background(),
				s.params.Timeout)
			defer cancel()
			rows, err := s.qr.SelectQueryContext(ctx, q)
			n, err := s.processSearchQuery(srm, rows, err, s2)
			if err != nil {
				errs <- err
			}
			s.logger.Debug("searcher finished", logSearcherFinished(s2, query, n)...)

		}(s1, wg1)
	}
	wg1.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}

	// return just the entities, without their granular or norm'd similarity scores
	es := make([]*api.EntityDetail, 0, limit)
	ess := srm.top(limit)
	s.logger.Debug("ranked search results", logSearchRanked(query, limit, ess)...)
	for _, eSim := range ess {
		es = append(es, eSim.E)
	}
	return es, nil
}

func (s *storer) AddPublicKeys(pkds []*api.PublicKeyDetail) error {
	if err := api.ValidatePublicKeyDetails(pkds); err != nil {
		return err
	}
	if len(pkds) > int(s.params.MaxBatchSize) {
		return storage.ErrMaxBatchSizeExceeded
	}
	q := psql.RunWith(s.db).
		Insert(fqPublicKeyDetailTable).
		Columns(pkdSQLCols...)
	for _, pkd := range pkds {
		q = q.Values(getPKDSQLValues(pkd)...)
	}
	s.logger.Debug("adding public keys to storage", logAddingPublicKeys(q, pkds)...)
	ctx, cancel := context.WithTimeout(context.Background(), s.params.Timeout)
	defer cancel()
	_, err := s.qr.InsertExecContext(ctx, q)
	if err != nil {
		return err
	}
	s.logger.Debug("added public keys to storage", logAddedPublicKeys(pkds)...)
	return nil
}

func (s *storer) GetPublicKeys(pks [][]byte) ([]*api.PublicKeyDetail, error) {
	if err := api.ValidatePublicKeys(pks); err != nil {
		return nil, err
	}
	if len(pks) > int(s.params.MaxBatchSize) {
		return nil, storage.ErrMaxBatchSizeExceeded
	}
	cols, _, _ := prepPKDScan()
	q := psql.RunWith(s.dbCache).
		Select(cols...).
		From(fqPublicKeyDetailTable).
		Where(sq.Eq{publicKeyCol: pks})
	s.logger.Debug("getting public keys from storage", logGettingPublicKeys(q, pks)...)
	pkds, err := s.getPKDsFromQuery(q, len(pks))
	if err != nil {
		return nil, err
	}
	s.logger.Debug("got public keys from storage", zap.Int(logNPublicKeys, len(pkds)))
	return orderPKDs(pkds, pks), nil
}

func (s *storer) GetEntityPublicKeys(
	entityID string, kt api.KeyType,
) ([]*api.PublicKeyDetail, error) {
	if entityID == "" {
		return nil, api.ErrEmptyEntityID
	}
	cols, _, _ := prepPKDScan()
	q := psql.RunWith(s.dbCache).
		Select(cols...).
		From(fqPublicKeyDetailTable).
		Where(sq.Eq{entityIDCol: entityID, keyTypeCol: kt.String()})
	s.logger.Debug("getting entity public keys from storage",
		logGettingEntityPubKeys(q, entityID)...)
	pkds, err := s.getPKDsFromQuery(q, storage.MaxEntityKeyTypeKeys)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("got entity public keys from storage",
		logGotEntityPubKeys(entityID, pkds)...)
	return pkds, nil
}

func (s *storer) CountEntityPublicKeys(entityID string, kt api.KeyType) (int, error) {
	if entityID == "" {
		return 0, api.ErrEmptyEntityID
	}
	q := psql.RunWith(s.dbCache).
		Select(count).
		From(fqPublicKeyDetailTable).
		Where(sq.Eq{
			entityIDCol: entityID,
			keyTypeCol:  kt.String(),
		})
	s.logger.Debug("counting public keys for entity",
		logCountingEntityPubKeys(q, entityID, kt)...)
	ctx, cancel := context.WithTimeout(context.Background(), s.params.Timeout)
	defer cancel()
	row := s.qr.SelectQueryRowContext(ctx, q)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	s.logger.Debug("counted public keys for entity",
		logCountEntityPubKeys(entityID, kt, count)...)
	return count, nil
}

func (s *storer) getPKDsFromQuery(q sq.SelectBuilder, size int) ([]*api.PublicKeyDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.params.Timeout)
	defer cancel()
	rows, err := s.qr.SelectQueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	pkds := make([]*api.PublicKeyDetail, size)
	i := 0
	for rows.Next() {
		_, dest, create := prepPKDScan()
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		pkds[i] = create()
		i++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pkds[:i], nil
}

func (s *storer) processSearchQuery(
	srm searchResultMerger, rows bstorage.QueryRows, err error, sch searcher,
) (int, error) {
	if err != nil {
		if err != context.DeadlineExceeded && err != sql.ErrNoRows {
			return 0, err
		}
		return 0, nil
	}
	n, err := srm.merge(rows, sch.name(), sch.entityType())
	if err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return n, err
	}
	if err := rows.Close(); err != nil {
		return n, err
	}
	return n, nil
}

func (s *storer) Close() error {
	return s.db.Close()
}
