package completion

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// MongoToken represents a parsed token from a Mongo query string.
type MongoToken struct {
	Value string
	Start int
	End   int
}

// MongoTokens parses a Mongo query into tokens, handling both shell format
// (db.collection.method(args)) and internal format (command collection {filter}).
func MongoTokens(query string) []MongoToken {
	runes := []rune(query)
	if shellTokens, ok := MongoShellTokens(runes); ok {
		return shellTokens
	}
	tokens := make([]MongoToken, 0, 8)
	start := -1
	depth := 0
	inQuote := false
	var quote rune
	for i, r := range runes {
		if inQuote {
			if r == quote && (i == 0 || runes[i-1] != '\\') {
				inQuote = false
			}
			continue
		}
		switch r {
		case '"', '\'':
			inQuote = true
			quote = r
			if start == -1 {
				start = i
			}
		case '{', '[':
			depth++
			if start == -1 {
				start = i
			}
		case '}', ']':
			if depth > 0 {
				depth--
			}
		}
		if start == -1 {
			if !unicode.IsSpace(r) {
				start = i
			}
			continue
		}
		if depth == 0 && unicode.IsSpace(r) {
			tokens = append(tokens, MongoToken{Value: string(runes[start:i]), Start: start, End: i})
			start = -1
		}
	}
	if start >= 0 {
		tokens = append(tokens, MongoToken{Value: string(runes[start:]), Start: start, End: len(runes)})
	}
	return tokens
}

// MongoShellTokens parses db.collection.method(arg0, arg1, ...) into virtual tokens.
func MongoShellTokens(runes []rune) ([]MongoToken, bool) {
	s := string(runes)
	if !strings.HasPrefix(s, "db.") {
		return nil, false
	}
	rest := s[3:]
	dotIdx := strings.Index(rest, ".")
	if dotIdx < 0 {
		return []MongoToken{
			{Value: "", Start: 0, End: 0},
			{Value: rest, Start: 3, End: len(runes)},
		}, true
	}
	collection := rest[:dotIdx]
	afterCollection := rest[dotIdx+1:]
	parenIdx := strings.Index(afterCollection, "(")
	if parenIdx < 0 {
		methodStart := 3 + dotIdx + 1
		return []MongoToken{
			{Value: afterCollection, Start: methodStart, End: len(runes)},
			{Value: collection, Start: 3, End: 3 + dotIdx},
		}, true
	}
	method := strings.ToLower(afterCollection[:parenIdx])
	methodStart := 3 + dotIdx + 1
	argsStart := methodStart + parenIdx + 1
	tokens := []MongoToken{
		{Value: method, Start: methodStart, End: methodStart + parenIdx},
		{Value: collection, Start: 3, End: 3 + dotIdx},
	}
	argsRunes := runes[argsStart:]
	argsEnd := len(argsRunes)
	if argsEnd > 0 && argsRunes[argsEnd-1] == ')' {
		argsEnd--
	}
	argsRunes = argsRunes[:argsEnd]
	if len(argsRunes) == 0 {
		tokens = append(tokens, MongoToken{Value: "", Start: argsStart, End: argsStart})
		return tokens, true
	}
	depth := 0
	inStr := false
	escape := false
	argStart := 0
	for i := 0; i < len(argsRunes); i++ {
		ch := argsRunes[i]
		if escape {
			escape = false
			continue
		}
		if ch == '\\' && inStr {
			escape = true
			continue
		}
		if ch == '"' {
			inStr = !inStr
			continue
		}
		if inStr {
			continue
		}
		switch ch {
		case '{', '[', '(':
			depth++
		case '}', ']', ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				argVal := strings.TrimSpace(string(argsRunes[argStart:i]))
				tokens = append(tokens, MongoToken{
					Value: argVal,
					Start: argsStart + argStart,
					End:   argsStart + i,
				})
				argStart = i + 1
			}
		}
	}
	argVal := strings.TrimSpace(string(argsRunes[argStart:]))
	tokens = append(tokens, MongoToken{
		Value: argVal,
		Start: argsStart + argStart,
		End:   argsStart + len(argsRunes),
	})
	return tokens, true
}

// MongoCollectionFromTokens extracts the collection name from parsed tokens.
func MongoCollectionFromTokens(tokens []MongoToken) string {
	if len(tokens) < 2 {
		return ""
	}
	return strings.TrimSpace(tokens[1].Value)
}

// MongoCommandItemsForCollection returns the top-level Mongo command completions.
func MongoCommandItemsForCollection(table string) []Item {
	if table == "" {
		table = "collection"
	}
	return []Item{
		{Label: "find", Detail: "query", InsertText: fmt.Sprintf("db.%s.find({})", table)},
		{Label: "aggregate", Detail: "query", InsertText: fmt.Sprintf("db.%s.aggregate([])", table)},
		{Label: "insertOne", Detail: "query", InsertText: fmt.Sprintf("db.%s.insertOne({})", table)},
		{Label: "updateOne", Detail: "query", InsertText: fmt.Sprintf("db.%s.updateOne({},{\"$set\":{}})", table)},
		{Label: "updateMany", Detail: "query", InsertText: fmt.Sprintf("db.%s.updateMany({},{\"$set\":{}})", table)},
		{Label: "deleteOne", Detail: "query", InsertText: fmt.Sprintf("db.%s.deleteOne({})", table)},
		{Label: "deleteMany", Detail: "query", InsertText: fmt.Sprintf("db.%s.deleteMany({})", table)},
		{Label: "countDocuments", Detail: "query", InsertText: fmt.Sprintf("db.%s.countDocuments({})", table)},
		{Label: "collections", Detail: "command", InsertText: "collections"},
	}
}

func MongoJSONTopLevelOperatorItems() []Item {
	return []Item{
		{Label: "$or", Detail: "operator", InsertText: `"$or":[{}]`},
		{Label: "$and", Detail: "operator", InsertText: `"$and":[{}]`},
		{Label: "$nor", Detail: "operator", InsertText: `"$nor":[{}]`},
		{Label: "$expr", Detail: "operator", InsertText: `"$expr":{}`},
	}
}

func MongoJSONUpdateOperatorItems() []Item {
	return []Item{
		{Label: "$set", Detail: "operator", InsertText: `"$set":{}`},
		{Label: "$unset", Detail: "operator", InsertText: `"$unset":{"field":""}`},
		{Label: "$inc", Detail: "operator", InsertText: `"$inc":{"field":1}`},
		{Label: "$push", Detail: "operator", InsertText: `"$push":{"field":""}`},
		{Label: "$pull", Detail: "operator", InsertText: `"$pull":{"field":""}`},
		{Label: "$addToSet", Detail: "operator", InsertText: `"$addToSet":{"field":""}`},
	}
}

func MongoAggregationStageItems() []Item {
	return []Item{
		{Label: "$match", Detail: "stage", InsertText: `{"$match":{}}`},
		{Label: "$group", Detail: "stage", InsertText: `{"$group":{"_id":"$field"}}`},
		{Label: "$project", Detail: "stage", InsertText: `{"$project":{"field":1}}`},
		{Label: "$sort", Detail: "stage", InsertText: `{"$sort":{"field":-1}}`},
		{Label: "$limit", Detail: "stage", InsertText: `{"$limit":20}`},
		{Label: "$skip", Detail: "stage", InsertText: `{"$skip":0}`},
		{Label: "$unwind", Detail: "stage", InsertText: `{"$unwind":"$field"}`},
		{Label: "$lookup", Detail: "stage", InsertText: `{"$lookup":{"from":"other","localField":"field","foreignField":"_id","as":"joined"}}`},
		{Label: "$addFields", Detail: "stage", InsertText: `{"$addFields":{"newField":"$existing"}}`},
		{Label: "$count", Detail: "stage", InsertText: `{"$count":"total"}`},
	}
}

func MongoJSONComparisonOperatorItems(field, fieldType, token string, cursor int) []Item {
	_, _, rawValue, preserveValue := MongoJSONOperatorPairBounds(token, cursor)
	operatorInsert := func(op, fallback string) string {
		if preserveValue {
			return fmt.Sprintf(`"%s":%s`, op, MongoTransformOperatorValue(op, rawValue, fieldType))
		}
		return fallback
	}
	value := MongoPlaceholderForType(fieldType)
	items := []Item{
		{Label: "$eq", Detail: "operator", InsertText: operatorInsert(`$eq`, fmt.Sprintf(`"$eq":%s`, value))},
		{Label: "$ne", Detail: "operator", InsertText: operatorInsert(`$ne`, fmt.Sprintf(`"$ne":%s`, value))},
		{Label: "$exists", Detail: "operator", InsertText: operatorInsert(`$exists`, `"$exists":true`)},
	}
	switch strings.ToLower(fieldType) {
	case "int", "uint", "float", "decimal", "number", "date", "datetime", "timestamp":
		items = append(items,
			Item{Label: "$gt", Detail: "operator", InsertText: operatorInsert(`$gt`, fmt.Sprintf(`"$gt":%s`, value))},
			Item{Label: "$gte", Detail: "operator", InsertText: operatorInsert(`$gte`, fmt.Sprintf(`"$gte":%s`, value))},
			Item{Label: "$lt", Detail: "operator", InsertText: operatorInsert(`$lt`, fmt.Sprintf(`"$lt":%s`, value))},
			Item{Label: "$lte", Detail: "operator", InsertText: operatorInsert(`$lte`, fmt.Sprintf(`"$lte":%s`, value))},
			Item{Label: "$in", Detail: "operator", InsertText: operatorInsert(`$in`, fmt.Sprintf(`"$in":[%s]`, value))},
			Item{Label: "$nin", Detail: "operator", InsertText: operatorInsert(`$nin`, fmt.Sprintf(`"$nin":[%s]`, value))},
		)
	case "string":
		items = append(items,
			Item{Label: "$regex", Detail: "operator", InsertText: operatorInsert(`$regex`, `"$regex":""`)},
			Item{Label: "$in", Detail: "operator", InsertText: operatorInsert(`$in`, `"$in":[""]`)},
		)
	}
	return items
}

func MongoJSONComparisonFieldContext(token string, cursor int) (string, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	match := regexp.MustCompile(`(?s)"([^"]+)"\s*:\s*\{\s*(?:"\$?[A-Za-z_]*)?$`).FindStringSubmatch(before)
	if len(match) != 2 {
		return "", false
	}
	if strings.HasPrefix(match[1], "$") {
		return "", false
	}
	return match[1], true
}

func MongoJSONOperatorBounds(token string, cursor int) (int, int, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	start := cursor
	for start > 0 {
		if runes[start-1] == '"' {
			break
		}
		if !(unicode.IsLetter(runes[start-1]) || runes[start-1] == '$' || runes[start-1] == '_') {
			return 0, 0, false
		}
		start--
	}
	if start >= len(runes) || runes[start] != '$' {
		if start+1 >= len(runes) || runes[start] != '"' || runes[start+1] != '$' {
			return 0, 0, false
		}
		start++
	}
	end := start
	for end < len(runes) && (unicode.IsLetter(runes[end]) || runes[end] == '$' || runes[end] == '_') {
		end++
	}
	if start == end {
		return 0, 0, false
	}
	return start - 1, min(len(runes), end+1), true
}

func MongoJSONOperatorPairBounds(token string, cursor int) (int, int, string, bool) {
	runes := []rune(token)
	start, end, ok := MongoJSONOperatorBounds(token, cursor)
	if !ok {
		return 0, 0, "", false
	}
	if end > len(runes) {
		end = len(runes)
	}
	i := end
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i >= len(runes) || runes[i] != ':' {
		return 0, 0, "", false
	}
	i++
	for i < len(runes) && unicode.IsSpace(runes[i]) {
		i++
	}
	if i >= len(runes) {
		return start, end, "", false
	}
	valueStart := i
	valueEnd := MongoJSONLiteralEnd(runes, valueStart)
	raw := strings.TrimSpace(string(runes[valueStart:valueEnd]))
	return start, valueEnd, raw, raw != ""
}

func MongoTransformOperatorValue(op, rawValue, fieldType string) string {
	raw := strings.TrimSpace(rawValue)
	if raw == "" {
		switch op {
		case "$exists":
			return "true"
		case "$in", "$nin":
			value := MongoPlaceholderForType(fieldType)
			return "[" + value + "]"
		default:
			return MongoPlaceholderForType(fieldType)
		}
	}
	switch op {
	case "$exists":
		if strings.EqualFold(raw, "true") || strings.EqualFold(raw, "false") {
			return strings.ToLower(raw)
		}
		return "true"
	case "$in", "$nin":
		if strings.HasPrefix(raw, "[") {
			return raw
		}
		return "[" + raw + "]"
	default:
		return raw
	}
}

func MongoJSONFieldItems(fields []string, fieldTypes map[string]string, token string, cursor int) ([]Item, bool) {
	if len(fields) == 0 {
		return nil, false
	}
	if !MongoLooksLikeFieldKeyContext(token, cursor) {
		return nil, false
	}
	items := make([]Item, 0, len(fields))
	for _, field := range fields {
		items = append(items, Item{
			Label:      field,
			Detail:     "field",
			InsertText: fmt.Sprintf(`"%s":%s`, field, MongoPlaceholderForType(fieldTypes[field])),
		})
	}
	return items, true
}

func MongoJSONUpdateFieldItems(fields []string, fieldTypes map[string]string, token string, cursor int) ([]Item, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	if !regexp.MustCompile(`(?s)"\$(?:set|unset|inc|push|pull|addToSet)"\s*:\s*\{\s*(?:"[^"]*)?$`).MatchString(before) {
		return nil, false
	}
	return MongoJSONFieldItems(fields, fieldTypes, token, cursor)
}

// MongoJSONObjectItems resolves field/operator items for JSON object contexts.
func MongoJSONObjectItems(command string, fields []string, fieldTypes map[string]string, token string, cursor int) ([]Item, bool) {
	if items, ok := MongoJSONUpdateFieldItems(fields, fieldTypes, token, cursor); ok {
		return items, true
	}
	if field, ok := MongoJSONComparisonFieldContext(token, cursor); ok {
		return MongoJSONComparisonOperatorItems(field, fieldTypes[field], token, cursor), true
	}
	if !MongoLooksLikeFieldKeyContext(token, cursor) {
		return nil, false
	}
	items, _ := MongoJSONFieldItems(fields, fieldTypes, token, cursor)
	switch command {
	case "update":
		items = append(MongoJSONUpdateOperatorItems(), items...)
	default:
		items = append(items, MongoJSONTopLevelOperatorItems()...)
	}
	return items, len(items) > 0
}

func MongoLooksLikeFieldKeyContext(token string, cursor int) bool {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	segment := strings.TrimSpace(string(runes[:cursor]))
	if segment == "" {
		return false
	}
	segment = strings.TrimRight(segment, " \t")
	if strings.HasSuffix(segment, "{") || strings.HasSuffix(segment, ",") || strings.HasSuffix(segment, `"`) {
		return true
	}
	if regexp.MustCompile(`(?s)[{,]\s*"[^"]*$`).MatchString(segment) {
		return true
	}
	return false
}

func MongoJSONKeyBounds(token string, cursor int) (int, int, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	idxs := regexp.MustCompile(`(?s)(^|[{,]\s*)("?[A-Za-z0-9_$]*)$`).FindStringSubmatchIndex(before)
	if len(idxs) != 6 {
		return 0, 0, false
	}
	start := idxs[4]
	end := cursor
	for end < len(runes) {
		r := runes[end]
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '$' {
			end++
			continue
		}
		if r == '"' {
			end++
		}
		break
	}
	return start, end, true
}

func MongoPlaceholderForType(fieldType string) string {
	switch strings.ToLower(fieldType) {
	case "objectid":
		return `{"$oid":"000000000000000000000000"}`
	case "date", "datetime", "timestamp":
		return `{"$date":"2026-01-01T00:00:00Z"}`
	case "array":
		return "[]"
	case "object", "document", "map", "mixed":
		return "{}"
	case "bool", "boolean", "int", "uint", "float", "decimal", "number", "null":
		return "null"
	default:
		return `""`
	}
}

func MongoTypedJSONLiteral(fieldType, raw string) string {
	trimmed := strings.TrimSpace(raw)
	kind := strings.ToLower(fieldType)

	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		if json.Valid([]byte(trimmed)) {
			return trimmed
		}
	}

	switch kind {
	case "objectid":
		if LooksLikeObjectIDHex(trimmed) {
			return fmt.Sprintf(`{"$oid":"%s"}`, trimmed)
		}
	case "date", "datetime", "timestamp":
		if t, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
			return fmt.Sprintf(`{"$date":"%s"}`, t.UTC().Format(time.RFC3339Nano))
		}
		if t, err := time.Parse(time.RFC3339, trimmed); err == nil {
			return fmt.Sprintf(`{"$date":"%s"}`, t.UTC().Format(time.RFC3339))
		}
	case "array":
		if strings.HasPrefix(trimmed, "[") && json.Valid([]byte(trimmed)) {
			return trimmed
		}
	case "object", "document", "map":
		if strings.HasPrefix(trimmed, "{") && json.Valid([]byte(trimmed)) {
			return trimmed
		}
	}

	switch strings.ToLower(fieldType) {
	case "bool", "boolean":
		if strings.EqualFold(trimmed, "true") {
			return "true"
		}
		if strings.EqualFold(trimmed, "false") {
			return "false"
		}
	case "int", "uint", "float", "decimal", "number":
		if _, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return trimmed
		}
	case "null":
		if strings.EqualFold(trimmed, "null") || strings.EqualFold(trimmed, "NULL") {
			return "null"
		}
	}
	if strings.EqualFold(trimmed, "null") || strings.EqualFold(trimmed, "NULL") {
		return "null"
	}
	return strconv.Quote(raw)
}

func LooksLikeObjectIDHex(s string) bool {
	if len(s) != 24 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

func MongoLiteralCandidates(fieldType string) []string {
	switch strings.ToLower(fieldType) {
	case "bool", "boolean":
		return []string{"true", "false", "null"}
	case "int", "uint", "float", "decimal", "number":
		return []string{"0", "1", "-1", "3.14", "null"}
	case "objectid":
		return []string{`{"$oid":"000000000000000000000000"}`, "null"}
	case "date", "datetime", "timestamp":
		return []string{`{"$date":"2026-01-01T00:00:00Z"}`, "null"}
	case "array":
		return []string{"[]", "[1,2,3]", "null"}
	case "object", "document", "map":
		return []string{"{}", `{"key":"value"}`, "null"}
	case "mixed":
		return []string{"true", "false", "0", "null", "{}", "[]"}
	default:
		return []string{"null"}
	}
}

func MongoFieldAndValuePrefix(token string, cursor int) (field string, valuePrefix string, ok bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	quoted := regexp.MustCompile(`"([^"]+)"\s*:\s*"([^"]*)$`).FindStringSubmatch(before)
	if len(quoted) == 3 {
		return quoted[1], quoted[2], true
	}
	bare := regexp.MustCompile(`"([^"]+)"\s*:\s*([^,\}\]\s]*)$`).FindStringSubmatch(before)
	if len(bare) == 3 {
		return bare[1], strings.TrimSpace(strings.TrimPrefix(bare[2], `"`)), true
	}
	return "", "", false
}

func MongoJSONValueBounds(token string, cursor int) (int, int, bool) {
	runes := []rune(token)
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(runes) {
		cursor = len(runes)
	}
	before := string(runes[:cursor])
	if idxs := regexp.MustCompile(`"([^"]+)"\s*:\s*"([^"]*)$`).FindStringSubmatchIndex(before); len(idxs) == 6 {
		contentStart := idxs[4]
		start := contentStart - 1
		end := cursor
		for end < len(runes) {
			if runes[end] == '"' && (end == 0 || runes[end-1] != '\\') {
				end++
				break
			}
			end++
		}
		return start, end, true
	}
	if idxs := regexp.MustCompile(`"([^"]+)"\s*:\s*([^,\}\]\s]*)$`).FindStringSubmatchIndex(before); len(idxs) == 6 {
		start := idxs[4]
		end := cursor
		for end < len(runes) && !strings.ContainsRune(",}]", runes[end]) && !unicode.IsSpace(runes[end]) {
			end++
		}
		return start, end, true
	}
	return 0, 0, false
}

func MongoJSONLiteralEnd(runes []rune, start int) int {
	if start >= len(runes) {
		return start
	}
	if runes[start] == '"' {
		end := start + 1
		for end < len(runes) {
			if runes[end] == '"' && runes[end-1] != '\\' {
				return end + 1
			}
			end++
		}
		return len(runes)
	}
	depthBrace := 0
	depthBracket := 0
	inQuote := false
	for end := start; end < len(runes); end++ {
		r := runes[end]
		if inQuote {
			if r == '"' && runes[end-1] != '\\' {
				inQuote = false
			}
			continue
		}
		switch r {
		case '"':
			inQuote = true
		case '{':
			depthBrace++
		case '}':
			if depthBrace == 0 && depthBracket == 0 {
				return end
			}
			if depthBrace > 0 {
				depthBrace--
			}
		case '[':
			depthBracket++
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
		case ',':
			if depthBrace == 0 && depthBracket == 0 {
				return end
			}
		}
	}
	return len(runes)
}

func MongoCompletionPrefix(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if idx := strings.LastIndex(token, `"`); idx >= 0 && idx+1 <= len(token) {
		return strings.ToLower(strings.TrimSpace(token[idx+1:]))
	}
	return strings.ToLower(strings.Trim(token, `"'`))
}

// --- Mongo completion engine (moved from update.go) ---

func mongoComplete(req Request) *Result {
	query := req.Query
	cursor := req.Cursor
	isShell := strings.HasPrefix(strings.TrimSpace(query), "db.")
	tokens := MongoTokens(query)

	tokenIdx := 0
	start := cursor
	end := cursor
	found := false

	for i, token := range tokens {
		if cursor >= token.Start && cursor <= token.End {
			tokenIdx = i
			start = token.Start
			end = token.End
			found = true
			break
		}
	}
	if !found {
		bestIdx := -1
		for i, token := range tokens {
			if token.Start > cursor {
				if bestIdx < 0 || token.Start < tokens[bestIdx].Start {
					bestIdx = i
				}
			}
		}
		if bestIdx >= 0 {
			tokenIdx = bestIdx
		} else if len(tokens) > 0 {
			tokenIdx = len(tokens)
		}
	}

	prefix := ""
	if start < cursor {
		prefix = MongoCompletionPrefix(query[start:cursor])
	}
	command := ""
	if len(tokens) > 0 {
		command = strings.ToLower(tokens[0].Value)
	}

	ctx := &Result{Start: start, End: end, Title: "Mongo Commands", Fallback: strings.TrimSpace(query[start:end])}
	var items []Item

	switch tokenIdx {
	case 0:
		ctx.Title = "Mongo Commands"
		table := ""
		if isShell {
			table = MongoCollectionFromTokens(tokens)
		}
		if table == "" {
			table = fallbackName(effectiveTable(req), "collection")
		}
		items = MongoCommandItemsForCollection(table)
		if isShell {
			ctx.Start = 0
			ctx.End = len([]rune(query))
			ctx.Fallback = query
		}

	case 1:
		ctx.Title = "Collections"
		items = mongoCollectionItems(req.Tables)
		if isShell {
			items = mongoShellCollectionItems(req.Tables, command, tokens)
			ctx.Start = 0
			ctx.End = len([]rune(query))
			ctx.Fallback = query
		}

	default:
		ctx.Title = "Mongo Arguments"
		collection := MongoCollectionFromTokens(tokens)
		tokenText := strings.TrimSpace(query[start:end])
		argResult := mongoArgumentItems(req, command, collection, tokenIdx, tokenText, cursor-start)
		items = argResult.items
		if argResult.needSchema != "" {
			ctx.NeedSchema = argResult.needSchema
		}
		if argResult.needValues != nil {
			ctx.NeedValues = argResult.needValues
		}

		if strings.HasPrefix(tokenText, "{") && len(tokenText) > 1 {
			if valueStart, valueEnd, ok := MongoJSONValueBounds(tokenText, cursor-start); ok {
				ctx.Title = "Mongo Value"
				ctx.Start = start + valueStart
				ctx.End = start + valueEnd
				ctx.Fallback = string([]rune(query)[ctx.Start:ctx.End])
			}
		}
		if command == "find" && tokenIdx >= 4 {
			ctx.Title = "Sort"
		}
		if strings.HasPrefix(tokenText, "{") && len(tokenText) > 1 {
			if field, ok := MongoJSONComparisonFieldContext(tokenText, cursor-start); ok && field != "" {
				if opStart, opEnd, _, opOK := MongoJSONOperatorPairBounds(tokenText, cursor-start); opOK {
					ctx.Title = "Mongo Operator"
					ctx.Start = start + opStart
					ctx.End = start + opEnd
					ctx.Fallback = strings.Trim(string([]rune(query)[ctx.Start:ctx.End]), `"`)
					prefix = ""
				}
			} else if keyStart, keyEnd, ok := MongoJSONKeyBounds(tokenText, cursor-start); ok {
				ctx.Title = "Mongo Field"
				ctx.Start = start + keyStart
				ctx.End = start + keyEnd
				ctx.Fallback = strings.Trim(string([]rune(query)[ctx.Start:ctx.End]), `"`)
				prefix = strings.ToLower(ctx.Fallback)
			}
		}
	}

	items = RankItems(prefix, items)
	if len(items) == 0 {
		return nil
	}
	ctx.Items = items
	return ctx
}

func mongoCollectionItems(tables []string) []Item {
	items := make([]Item, 0, len(tables))
	for _, name := range tables {
		items = append(items, Item{Label: name, Detail: "collection", InsertText: name})
	}
	return items
}

func mongoShellCollectionItems(tables []string, method string, tokens []MongoToken) []Item {
	if method == "" {
		method = "find"
	}
	shellMethod := method
	switch method {
	case "find":
		shellMethod = "find"
	case "aggregate":
		shellMethod = "aggregate"
	case "insert":
		shellMethod = "insertOne"
	case "update":
		shellMethod = "updateOne"
	case "delete":
		shellMethod = "deleteOne"
	case "count":
		shellMethod = "countDocuments"
	}
	args := ""
	if len(tokens) > 2 {
		argParts := make([]string, 0, len(tokens)-2)
		for _, t := range tokens[2:] {
			argParts = append(argParts, t.Value)
		}
		args = strings.Join(argParts, ", ")
	}
	if args == "" {
		args = "{}"
	}
	items := make([]Item, 0, len(tables))
	for _, name := range tables {
		items = append(items, Item{
			Label:      name,
			Detail:     "collection",
			InsertText: fmt.Sprintf("db.%s.%s(%s)", name, shellMethod, args),
		})
	}
	return items
}

type mongoArgResult struct {
	items      []Item
	needSchema string
	needValues *ValueRequest
}

func mongoArgumentItems(req Request, command, collection string, tokenIdx int, token string, tokenCursor int) mongoArgResult {
	// Only use req.Schema if it matches the collection we're completing for.
	// The caller may have resolved schema for InferredTable which could differ
	// from the token-parsed collection in edge cases.
	schema := req.Schema
	if schema != nil && collection != "" && !strings.EqualFold(schema.Name, collection) {
		schema = nil
	}
	fields, fieldTypes := schemaFields(schema)
	filterField := fallbackName(preferredFilterColumnFromFields(fields), "column_name")
	groupField := fallbackName(preferredCategoricalColumnFromFields(fields, fieldTypes), "column_name")

	var needSchema string
	if len(fields) == 0 && collection != "" && schema == nil {
		needSchema = collection
	}

	filterItems := func() []Item {
		items := []Item{{Label: "empty filter", Detail: "json", InsertText: "{}"}}
		items = append(items, MongoJSONTopLevelOperatorItems()...)
		if len(fields) == 0 {
			items = append(items, Item{Label: "field filter", Detail: "json", InsertText: fmt.Sprintf(`{"%s":%s}`, filterField, MongoPlaceholderForType(""))})
			return items
		}
		for _, field := range fields {
			items = append(items, Item{
				Label:      field,
				Detail:     "field",
				InsertText: fmt.Sprintf(`{"%s":%s}`, field, MongoPlaceholderForType(fieldTypes[field])),
			})
		}
		return items
	}

	sortItems := func() []Item {
		if len(fields) == 0 {
			return []Item{
				{Label: "recent sort", Detail: "json", InsertText: fmt.Sprintf(`{"%s":-1}`, groupField)},
				{Label: "ascending sort", Detail: "json", InsertText: fmt.Sprintf(`{"%s":1}`, filterField)},
			}
		}
		items := make([]Item, 0, len(fields)*2)
		for _, field := range fields {
			items = append(items,
				Item{Label: field + " desc", Detail: "sort", InsertText: fmt.Sprintf(`{"%s":-1}`, field)},
				Item{Label: field + " asc", Detail: "sort", InsertText: fmt.Sprintf(`{"%s":1}`, field)},
			)
		}
		return items
	}

	trimmedToken := strings.TrimSpace(token)
	if strings.HasPrefix(trimmedToken, "{") && len(trimmedToken) > 1 {
		if valueItems, valueReq := mongoJSONValueItems(req, collection, fields, fieldTypes, token, tokenCursor); valueItems != nil {
			return mongoArgResult{items: valueItems, needSchema: needSchema, needValues: valueReq}
		}
		if cItems, ok := MongoJSONObjectItems(command, fields, fieldTypes, token, tokenCursor); ok {
			keyItems := cItems
			if len(fields) == 0 && needSchema != "" {
				hint := Item{Label: "loading fields…", Detail: collection, InsertText: ""}
				keyItems = append([]Item{hint}, keyItems...)
			}
			return mongoArgResult{items: keyItems, needSchema: needSchema}
		}
	}

	switch command {
	case "find":
		if tokenIdx == 2 {
			return mongoArgResult{items: filterItems(), needSchema: needSchema}
		}
		if tokenIdx == 3 {
			if strings.HasPrefix(strings.TrimSpace(token), "{") {
				return mongoArgResult{items: sortItems(), needSchema: needSchema}
			}
			return mongoArgResult{items: []Item{
				{Label: "limit 20", Detail: "limit", InsertText: "20"},
				{Label: "limit 50", Detail: "limit", InsertText: "50"},
				{Label: "limit 100", Detail: "limit", InsertText: "100"},
			}}
		}
		return mongoArgResult{items: sortItems(), needSchema: needSchema}
	case "aggregate":
		return mongoArgResult{items: []Item{
			{Label: "match + limit", Detail: "pipeline", InsertText: fmt.Sprintf(`[{"$match":{"%s":%s}},{"$limit":20}]`, filterField, MongoPlaceholderForType(fieldTypes[filterField]))},
			{Label: "group + count", Detail: "pipeline", InsertText: fmt.Sprintf(`[{"$group":{"_id":"$%s","count":{"$sum":1}}},{"$sort":{"count":-1}},{"$limit":20}]`, groupField)},
		}}
	case "insert":
		return mongoArgResult{items: []Item{
			{Label: "document", Detail: "json", InsertText: fmt.Sprintf(`{"%s":%s}`, filterField, MongoPlaceholderForType(fieldTypes[filterField]))},
		}}
	case "update":
		if tokenIdx == 2 {
			return mongoArgResult{items: filterItems(), needSchema: needSchema}
		}
		if tokenIdx == 3 {
			return mongoArgResult{items: []Item{
				{Label: "$set", Detail: "json", InsertText: fmt.Sprintf(`{"$set":{"%s":%s}}`, filterField, MongoPlaceholderForType(fieldTypes[filterField]))},
			}}
		}
		return mongoArgResult{items: []Item{
			{Label: "many", Detail: "token", InsertText: "many"},
		}}
	case "delete":
		if tokenIdx == 2 {
			return mongoArgResult{items: filterItems(), needSchema: needSchema}
		}
		return mongoArgResult{items: []Item{
			{Label: "many", Detail: "token", InsertText: "many"},
		}}
	case "count":
		return mongoArgResult{items: filterItems(), needSchema: needSchema}
	default:
		return mongoArgResult{needSchema: needSchema}
	}
}

func mongoJSONValueItems(req Request, collection string, _ []string, fieldTypes map[string]string, token string, cursor int) ([]Item, *ValueRequest) {
	if _, ok := MongoJSONComparisonFieldContext(token, cursor); ok {
		return nil, nil
	}
	col, prefix, ok := MongoFieldAndValuePrefix(token, cursor)
	if !ok || col == "" || strings.HasPrefix(col, "$") || strings.HasPrefix(strings.TrimSpace(prefix), "$") {
		return nil, nil
	}
	if collection == "" {
		return nil, nil
	}

	fieldType := fieldTypes[col]
	key := ValueCacheKey(collection, col)
	var values []string
	cached := false
	if req.ValueCache != nil {
		values, cached = req.ValueCache[key]
	}

	var needValues *ValueRequest
	if !cached {
		needValues = &ValueRequest{Table: collection, Column: col}
	}

	literals := MongoLiteralCandidates(fieldType)
	items := make([]Item, 0, len(values)+len(literals)+1)
	for _, literal := range literals {
		items = append(items, Item{Label: literal, Detail: col, InsertText: literal})
	}
	for _, v := range values {
		items = append(items, Item{Label: v, Detail: col, InsertText: MongoTypedJSONLiteral(fieldType, v)})
	}
	items = RankItems(strings.ToLower(prefix), items)
	if !cached && len(items) == 0 {
		items = append(items, Item{Label: "loading…", Detail: "fetching samples", InsertText: token})
	}
	if len(items) == 0 {
		items = append(items, Item{Label: "(no samples)", Detail: col, InsertText: token})
	}
	return items, needValues
}

func mongoFieldType(schema *SchemaInfo, field string) string {
	if schema == nil || field == "" {
		return ""
	}
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, field) {
			return strings.ToLower(col.Type)
		}
	}
	return ""
}
