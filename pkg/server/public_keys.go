package server

import (
	"bytes"
	"container/heap"
	"crypto/hmac"
	"crypto/sha256"
	"io"

	cerrors "github.com/drausin/libri/libri/common/errors"
	api "github.com/elixirhealth/entity/pkg/entityapi"
)

const (
	sortEntropyBytes = 4
)

// getOrderedLimit returns the top {{ limit }} PKDs, ordered by the SHA256-HMAC with the given mac
// key.
func getOrderedLimit(pkds []*api.PublicKeyDetail, macKey []byte, limit int) []*api.PublicKeyDetail {
	macer := hmac.New(sha256.New, macKey)
	pkdMACs := &sortablePublicKeyDetails{}
	heap.Init(pkdMACs)
	for _, pkd := range pkds {
		macer.Reset()
		_, err := macer.Write(pkd.PublicKey)
		cerrors.MaybePanic(err) // should never happen b/c sha256.Write always returns nil
		pkdMAC := &sortablePublicKeyDetail{
			pkd:    pkd,
			sortBy: macer.Sum(nil),
		}
		if pkdMACs.Len() < limit ||
			bytes.Compare(pkdMACs.Peak().sortBy, pkdMAC.sortBy) < 0 {
			heap.Push(pkdMACs, pkdMAC)
		}
		if pkdMACs.Len() > limit {
			heap.Pop(pkdMACs)
		}
	}
	top := make([]*api.PublicKeyDetail, pkdMACs.Len())
	for i := len(top) - 1; i >= 0; i-- {
		top[i] = heap.Pop(pkdMACs).(*sortablePublicKeyDetail).pkd
	}
	return top
}

// sampleWithoutReplacement returns n random samples from pkds via the first n elements of the
// permuted pkds list. If n < len(pkds), it returns all the elements of the list. The order of the
// returned list is arbitrary.
func sampleWithoutReplacement(
	pkds []*api.PublicKeyDetail, rng io.Reader, n int,
) []*api.PublicKeyDetail {
	if n >= len(pkds) {
		return pkds
	}
	ordered := &sortablePublicKeyDetails{}
	for _, pkd := range pkds {
		sortBy := make([]byte, sortEntropyBytes)
		_, err := rng.Read(sortBy)
		cerrors.MaybePanic(err) // should never happen
		heap.Push(ordered, &sortablePublicKeyDetail{
			pkd:    pkd,
			sortBy: sortBy,
		})
	}
	sample := make([]*api.PublicKeyDetail, n)
	for i := 0; i < n; i++ {
		spkd := heap.Pop(ordered).(*sortablePublicKeyDetail)
		sample[i] = spkd.pkd
	}
	return sample
}

type sortablePublicKeyDetail struct {
	pkd    *api.PublicKeyDetail
	sortBy []byte
}

type sortablePublicKeyDetails []*sortablePublicKeyDetail

func (m sortablePublicKeyDetails) Len() int {
	return len(m)
}

func (m sortablePublicKeyDetails) Less(i, j int) bool {
	return bytes.Compare(m[i].sortBy, m[j].sortBy) < 0
}

func (m sortablePublicKeyDetails) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m *sortablePublicKeyDetails) Push(x interface{}) {
	*m = append(*m, x.(*sortablePublicKeyDetail))
}

func (m *sortablePublicKeyDetails) Pop() interface{} {
	old := *m
	n := len(old)
	x := old[n-1]
	*m = old[0 : n-1]
	return x
}

func (m sortablePublicKeyDetails) Peak() *sortablePublicKeyDetail {
	return m[0]
}
