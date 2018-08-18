package postgres

import (
	"encoding/hex"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
)

func orderPKDs(pkds []*api.PublicKeyDetail, byPKs [][]byte) []*api.PublicKeyDetail {
	pkdsMap := make(map[string]*api.PublicKeyDetail)
	for _, pkd := range pkds {
		pkHex := hex.EncodeToString(pkd.PublicKey)
		pkdsMap[pkHex] = pkd
	}
	ordered := make([]*api.PublicKeyDetail, 0, len(pkds))
	for _, byPK := range byPKs {
		pkHex := hex.EncodeToString(byPK)
		if pkd, in := pkdsMap[pkHex]; in {
			ordered = append(ordered, pkd)
		}
	}
	return ordered
}

var pkdSQLCols = []string{
	publicKeyCol,
	keyTypeCol,
	entityIDCol,
}

func getPKDSQLValues(pkd *api.PublicKeyDetail) []interface{} {
	return []interface{}{
		pkd.PublicKey,
		pkd.KeyType.String(),
		pkd.EntityId,
	}
}

func prepPKDScan() ([]string, []interface{}, func() *api.PublicKeyDetail) {
	pkd := &api.PublicKeyDetail{}
	keyTypeStr := pkd.KeyType.String()
	cols, dests := bstorage.SplitColDests(0, []*bstorage.ColDest{
		{publicKeyCol, &pkd.PublicKey},
		{keyTypeCol, &keyTypeStr},
		{entityIDCol, &pkd.EntityId},
	})
	return cols, dests, func() *api.PublicKeyDetail {
		pkd.PublicKey = *dests[0].(*[]byte)
		pkd.KeyType = api.KeyType(api.KeyType_value[*dests[1].(*string)])
		pkd.EntityId = *dests[2].(*string)
		return pkd
	}
}
