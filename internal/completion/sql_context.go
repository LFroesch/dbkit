package completion

import "strings"

func InSelectList(before string) bool {
	selectIdx := LastKeyword(before, "select")
	if selectIdx < 0 {
		return false
	}
	afterSelect := before[selectIdx:]
	for _, blocker := range []string{" from ", " where ", " group by ", " order by ", " limit ", ";"} {
		if idx := strings.LastIndex(afterSelect, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InWhereClause(before string) bool {
	lastWhere := max(LastKeyword(before, " where "), LastKeyword(before, "where "))
	lastAnd := LastKeyword(before, " and ")
	lastOr := LastKeyword(before, " or ")
	lastOn := LastKeyword(before, " on ")
	lastHaving := LastKeyword(before, " having ")
	start := max(max(lastWhere, lastAnd), max(max(lastOr, lastOn), lastHaving))
	if start < 0 {
		return false
	}
	for _, blocker := range []string{" = ", " != ", " > ", " < ", " like ", " in ", " is ", "\n"} {
		if idx := strings.LastIndex(before, blocker); idx > start {
			return false
		}
	}
	return true
}

func InHavingClause(before string) bool {
	havingIdx := LastKeyword(before, " having ")
	if havingIdx < 0 {
		return false
	}
	after := before[havingIdx:]
	for _, blocker := range []string{" order by ", " limit ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InOrderByList(before string) bool {
	orderIdx := LastKeyword(before, " order by ")
	if orderIdx < 0 {
		return false
	}
	after := before[orderIdx:]
	for _, blocker := range []string{" limit ", " where ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InLimitValue(before string) bool {
	limitIdx := LastKeyword(before, " limit ")
	if limitIdx < 0 {
		return false
	}
	after := before[limitIdx+len(" limit "):]
	for _, blocker := range []string{";", "\n"} {
		if strings.Contains(after, blocker) {
			return false
		}
	}
	return true
}

func OrderByWantsDirection(before string) bool {
	orderIdx := LastKeyword(before, " order by ")
	if orderIdx < 0 {
		return false
	}
	after := strings.TrimSpace(before[orderIdx+len(" order by "):])
	if after == "" {
		return false
	}
	after = strings.TrimRight(after, " \t")
	if strings.HasSuffix(after, ",") {
		return false
	}
	parts := strings.Fields(strings.ReplaceAll(after, ",", " "))
	if len(parts) == 0 {
		return false
	}
	last := strings.ToLower(parts[len(parts)-1])
	switch last {
	case "asc", "desc", "nulls", "first", "last":
		return false
	default:
		return true
	}
}

func InGroupByList(before string) bool {
	groupIdx := LastKeyword(before, " group by ")
	if groupIdx < 0 {
		return false
	}
	after := before[groupIdx:]
	for _, blocker := range []string{" order by ", " limit ", ";"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InInsertColumnList(before string) bool {
	insertIdx := LastKeyword(before, "insert into ")
	valuesIdx := LastKeyword(before, " values")
	openParen := strings.LastIndex(before, "(")
	closeParen := strings.LastIndex(before, ")")
	return insertIdx >= 0 && openParen > insertIdx && openParen > closeParen && valuesIdx < openParen
}

func InInsertValuesList(before string) bool {
	insertIdx := LastKeyword(before, "insert into ")
	valuesIdx := LastKeyword(before, " values")
	openParen := strings.LastIndex(before, "(")
	closeParen := strings.LastIndex(before, ")")
	return insertIdx >= 0 && valuesIdx > insertIdx && openParen > valuesIdx && openParen > closeParen
}

func InUpdateSetList(before string) bool {
	updateIdx := LastKeyword(before, "update ")
	setIdx := LastKeyword(before, " set ")
	if updateIdx < 0 || setIdx < updateIdx {
		return false
	}
	lastWhere := LastKeyword(before, " where ")
	if lastWhere > setIdx {
		return false
	}
	for _, blocker := range []string{" = ", " != ", " > ", " < ", " like ", "\n"} {
		if idx := strings.LastIndex(before, blocker); idx > setIdx {
			return false
		}
	}
	return true
}

func InFromTable(before string) bool {
	fromIdx := LastKeyword(before, " from ")
	if fromIdx < 0 {
		return false
	}
	after := before[fromIdx:]
	for _, blocker := range []string{" where ", " join ", " group by ", " order by ", " limit ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InJoinTable(before string) bool {
	joinIdx := LastKeyword(before, " join ")
	if joinIdx < 0 {
		return false
	}
	after := before[joinIdx:]
	for _, blocker := range []string{" on ", " where ", " group by ", " order by ", " limit ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InUpdateTable(before string) bool {
	updateIdx := LastKeyword(before, "update ")
	if updateIdx < 0 {
		return false
	}
	after := before[updateIdx:]
	for _, blocker := range []string{" set ", " where ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InInsertIntoTable(before string) bool {
	insertIdx := LastKeyword(before, "insert into ")
	if insertIdx < 0 {
		return false
	}
	after := before[insertIdx:]
	for _, blocker := range []string{"(", " values", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}

func InDeleteFromTable(before string) bool {
	deleteIdx := LastKeyword(before, "delete from ")
	if deleteIdx < 0 {
		return false
	}
	after := before[deleteIdx:]
	for _, blocker := range []string{" where ", ";", "\n"} {
		if idx := strings.LastIndex(after, blocker); idx >= 0 {
			return false
		}
	}
	return true
}
