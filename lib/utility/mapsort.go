package utility

import (
	"sort"
	"strings"
)

type MapSorter struct {
	OrderBy string
	Items   []MapItem
}
type MapItem struct {
	Key string
	Val interface{}
}

func NewMapSorter(m map[string]interface{}, o string) MapSorter {
	ms := MapSorter{}
	ms.OrderBy = o

	for k, v := range m {
		ms.Items = append(ms.Items, MapItem{k, v})
	}

	return ms
}

func (ms MapSorter) Len() int {
	return len(ms.Items)
}

//按键排序
func (ms MapSorter) Less(i, j int) bool {
	if ms.OrderBy == "asc" {
		return strings.Compare(ms.Items[i].Key, ms.Items[j].Key) <= 0
	}
	return strings.Compare(ms.Items[i].Key, ms.Items[j].Key) > 0
}

func (ms MapSorter) Swap(i, j int) {
	ms.Items[i], ms.Items[j] = ms.Items[j], ms.Items[i]
}

func MapSortByKey(list map[string]interface{}, orderBy string) []MapItem {
	ms := NewMapSorter(list, orderBy)
	sort.Sort(ms)

	return ms.Items
}
