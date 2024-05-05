package types

import (
	"github.com/c9s/bbgo/pkg/fixedpoint"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type PriceVolumeTree struct {
	Tree *rbt.Tree
}

func NewPriceVolumeTree(descending bool) *PriceVolumeTree {
	if descending {
		return &PriceVolumeTree{
			Tree: &rbt.Tree{
				Comparator: IntDescendingComparator,
			},
		}
	}
	return &PriceVolumeTree{
		Tree: &rbt.Tree{
			Comparator: IntAscendingComparator,
		},
	}
}

func (t *PriceVolumeTree) Set(pv PriceVolume) {
	t.Tree.Put(int(pv.Price), pv)
}

func (t *PriceVolumeTree) Remove(pv PriceVolume) {
	t.Tree.Remove(int(pv.Price))
}

func (t *PriceVolumeTree) Get(Price fixedpoint.Value) (PriceVolume, bool) {
	v, found := t.Tree.Get(int(Price))
	if !found {
		return PriceVolume{}, false
	}

	return v.(PriceVolume), true
}

func (t *PriceVolumeTree) LoadPriceVolumeSlice(pvs PriceVolumeSlice) {
	for _, pv := range pvs {
		t.Set(pv)
	}
}

func (t *PriceVolumeTree) GetPriceVolumeSlice() PriceVolumeSlice {
	pvs := make(PriceVolumeSlice, 0, t.Tree.Size())
	for _, v := range t.Tree.Values() {
		pvs = append(pvs, v.(PriceVolume))
	}
	return pvs
}

func IntAscendingComparator(a, b interface{}) int {
	aAsserted := a.(int)
	bAsserted := b.(int)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func IntDescendingComparator(a, b interface{}) int {
	aAsserted := a.(int)
	bAsserted := b.(int)
	switch {
	case aAsserted < bAsserted:
		return 1
	case aAsserted > bAsserted:
		return -1
	default:
		return 0
	}
}
