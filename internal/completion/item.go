package completion

import (
	"sort"
	"strings"
)

// Item is a single completion candidate.
type Item struct {
	Label      string
	Detail     string
	InsertText string
	Selected   bool // used by multi-select picker in the UI
}

// RankItems returns items ordered by relevance to prefix.
// Exact prefix > contains > fuzzy. On empty prefix, returns a copy of items.
func RankItems(prefix string, items []Item) []Item {
	return rankItems(prefix, items, false)
}

// RankItemsKeepAll returns all items ordered by relevance to prefix.
// Non-matching items remain available after the strongest matches.
func RankItemsKeepAll(prefix string, items []Item) []Item {
	return rankItems(prefix, items, true)
}

func rankItems(prefix string, items []Item, keepAll bool) []Item {
	if len(items) == 0 {
		return nil
	}
	if prefix == "" {
		return append([]Item(nil), items...)
	}
	type ranked struct {
		item  Item
		score int
	}
	rankedItems := make([]ranked, 0, len(items))
	for _, item := range items {
		label := strings.ToLower(item.Label)
		insert := strings.ToLower(item.InsertText)
		score := -1
		switch {
		case strings.HasPrefix(label, prefix), strings.HasPrefix(insert, prefix):
			score = 0
		case strings.Contains(label, prefix), strings.Contains(insert, prefix):
			score = 1
		case FuzzyMatch(label, prefix), FuzzyMatch(insert, prefix):
			score = 2
		}
		if score >= 0 || keepAll {
			if score < 0 {
				score = 3
			}
			rankedItems = append(rankedItems, ranked{item: item, score: score})
		}
	}
	if len(rankedItems) == 0 {
		return RankItems("", items)
	}
	sort.SliceStable(rankedItems, func(i, j int) bool {
		if rankedItems[i].score != rankedItems[j].score {
			return rankedItems[i].score < rankedItems[j].score
		}
		return strings.ToLower(rankedItems[i].item.Label) < strings.ToLower(rankedItems[j].item.Label)
	})
	out := make([]Item, 0, len(rankedItems))
	for _, item := range rankedItems {
		out = append(out, item.item)
	}
	return out
}
