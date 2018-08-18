package server

import (
	"container/heap"
	"encoding/hex"
	"math/rand"
	"testing"

	api "github.com/elixirhealth/entity/pkg/entityapi"
	"github.com/stretchr/testify/assert"
)

func TestGetOrderedLimit(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	pkds := api.NewTestPublicKeyDetails(rng, 64)
	macKey1, macKey2 := []byte("entity ID 1"), []byte("entity ID 2")

	limit1 := 1
	r1 := getOrderedLimit(pkds, macKey1, limit1)
	assert.Equal(t, limit1, len(r1))

	limit2 := 2
	r2 := getOrderedLimit(pkds, macKey1, limit2)
	assert.Equal(t, limit2, len(r2))
	assert.Equal(t, r1, r2[:limit1])

	limit3 := 4
	r3 := getOrderedLimit(pkds, macKey1, limit3)
	assert.Equal(t, r2, r3[:limit2])

	limit4 := 8
	r4 := getOrderedLimit(pkds, macKey1, limit4)
	assert.Equal(t, r3, r4[:limit3])

	r5 := getOrderedLimit(pkds, macKey1, limit4)
	assert.Equal(t, r4, r5)

	r6 := getOrderedLimit(pkds, macKey2, 8)
	assert.NotEqual(t, r5, r6)
}

func TestSampleWithoutReplacement(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	pkds := api.NewTestPublicKeyDetails(rng, 16)
	n := 8

	sample1 := sampleWithoutReplacement(pkds, rng, n)
	assert.Equal(t, n, len(sample1))

	// check uniqueness
	pks := map[string]struct{}{}
	for _, pkd := range sample1 {
		pkHex := hex.EncodeToString(pkd.PublicKey)
		_, in := pks[pkHex]
		assert.False(t, in)
		pks[pkHex] = struct{}{}
	}

	// check diff sample
	sample2 := sampleWithoutReplacement(pkds, rng, n)
	assert.Equal(t, n, len(sample2))
	assert.NotEqual(t, sample1, sample2)

	sample3 := sampleWithoutReplacement(pkds, rng, 32)
	assert.Equal(t, len(pkds), len(sample3))
}

func TestSortablePublicKeyDetails(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	pkd1 := api.NewTestPublicKeyDetail(rng)
	pkd2 := api.NewTestPublicKeyDetail(rng)
	pkd3 := api.NewTestPublicKeyDetail(rng)
	pkd4 := api.NewTestPublicKeyDetail(rng)
	pkd5 := api.NewTestPublicKeyDetail(rng)
	spkds := &sortablePublicKeyDetails{
		{pkd: pkd1, sortBy: []byte{1}},
		{pkd: pkd2, sortBy: []byte{3}},
		{pkd: pkd3, sortBy: []byte{4}},
		{pkd: pkd4, sortBy: []byte{2}},
	}

	heap.Init(spkds)
	heap.Push(spkds, &sortablePublicKeyDetail{
		pkd:    pkd5,
		sortBy: []byte{0},
	})

	assert.Equal(t, pkd5, spkds.Peak().pkd)
	assert.Equal(t, pkd5, heap.Pop(spkds).(*sortablePublicKeyDetail).pkd)
	assert.Equal(t, pkd1, heap.Pop(spkds).(*sortablePublicKeyDetail).pkd)
	assert.Equal(t, pkd4, heap.Pop(spkds).(*sortablePublicKeyDetail).pkd)
	assert.Equal(t, pkd2, heap.Pop(spkds).(*sortablePublicKeyDetail).pkd)
	assert.Equal(t, pkd3, spkds.Pop().(*sortablePublicKeyDetail).pkd)
}
