package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	mongoConnectTimeout = 8 * time.Second
	mongoOpTimeout      = 15 * time.Second
	mongoSampleLimit    = int64(100)
	mongoDefaultLimit   = int64(100)
	mongoMaxLimit       = int64(1000)
)

// MongoDB implements DB for MongoDB.
type MongoDB struct {
	uri    string
	dbName string
	client *mongo.Client
	db     *mongo.Database
}

func (d *MongoDB) Type() string { return "mongo" }
func (d *MongoDB) DSN() string  { return d.uri }

func (d *MongoDB) Connect() error {
	dbName, err := parseMongoDBName(d.uri)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(d.uri))
	if err != nil {
		return err
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return err
	}

	d.client = client
	d.dbName = dbName
	d.db = client.Database(dbName)
	return nil
}

func (d *MongoDB) Close() {
	if d.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = d.client.Disconnect(ctx)
	d.client = nil
	d.db = nil
}

func (d *MongoDB) Ping() error {
	if d.client == nil {
		return fmt.Errorf("not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	return d.client.Ping(ctx, readpref.Primary())
}

func (d *MongoDB) GetTables() ([]string, error) {
	if d.db == nil {
		return nil, fmt.Errorf("not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	names, err := d.db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (d *MongoDB) GetTableSchema(collection string) (*TableSchema, error) {
	if d.db == nil {
		return nil, fmt.Errorf("not connected")
	}
	coll := d.db.Collection(collection)

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	rowCount, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	findOpts := options.Find().SetLimit(mongoSampleLimit)
	cur, err := coll.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	type fieldStat struct {
		types map[string]struct{}
		seen  int
	}
	stats := map[string]*fieldStat{}
	samples := 0

	for cur.Next(ctx) {
		samples++
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		for key, val := range doc {
			fs, ok := stats[key]
			if !ok {
				fs = &fieldStat{types: map[string]struct{}{}}
				stats[key] = fs
			}
			fs.seen++
			fs.types[mongoTypeName(val)] = struct{}{}
		}
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	schema := &TableSchema{Name: collection, RowCount: rowCount}
	if len(stats) == 0 {
		return schema, nil
	}

	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Keep Mongo's primary key first for consistency.
	if contains(keys, "_id") {
		keys = append([]string{"_id"}, remove(keys, "_id")...)
	}

	for _, key := range keys {
		fs := stats[key]
		typeName := "unknown"
		if len(fs.types) == 1 {
			for t := range fs.types {
				typeName = t
			}
		} else if len(fs.types) > 1 {
			typeName = "mixed"
		}

		schema.Columns = append(schema.Columns, ColumnInfo{
			Name:       key,
			Type:       typeName,
			Nullable:   fs.seen < samples,
			PrimaryKey: key == "_id",
		})
	}

	return schema, nil
}

func (d *MongoDB) RunQuery(query string) (*QueryResult, error) {
	if d.db == nil {
		return nil, fmt.Errorf("not connected")
	}

	q := strings.TrimSpace(query)

	if strings.EqualFold(q, "collections") || strings.EqualFold(q, "show collections") {
		names, err := d.GetTables()
		if err != nil {
			return nil, err
		}
		rows := make([][]string, 0, len(names))
		for _, n := range names {
			rows = append(rows, []string{n})
		}
		return &QueryResult{Columns: []string{"collection"}, Rows: rows}, nil
	}

	internal, ok := parseShellQuery(q)
	if !ok {
		return nil, fmt.Errorf("invalid query — use: db.collection.method({...}) or \"collections\"")
	}

	cmd, rest := nextWord(internal)
	switch strings.ToLower(cmd) {
	case "find":
		return d.runFind(rest)
	case "aggregate":
		return d.runAggregate(rest)
	case "count":
		return d.runCount(rest)
	case "insert":
		return d.runInsert(rest)
	case "update":
		return d.runUpdate(rest)
	case "delete":
		return d.runDelete(rest)
	default:
		return nil, fmt.Errorf("unsupported method %q", cmd)
	}
}

func (d *MongoDB) runFind(rest string) (*QueryResult, error) {
	collection, tail := nextWord(rest)
	if collection == "" {
		return nil, fmt.Errorf("usage: find <collection> [filter-json] [limit] [sort-json]")
	}

	filter := bson.M{}
	limit := mongoDefaultLimit
	var sort bson.D
	tail = strings.TrimSpace(tail)

	// Optional filter JSON.
	if startsWithJSON(tail) {
		jsonArg, remaining, err := extractJSONArg(tail)
		if err != nil {
			return nil, fmt.Errorf("invalid filter json: %w", err)
		}
		if err := bson.UnmarshalExtJSON([]byte(jsonArg), true, &filter); err != nil {
			return nil, fmt.Errorf("invalid filter json: %w", err)
		}
		tail = strings.TrimSpace(remaining)
	}

	// Optional limit (any bare integer).
	if tail != "" && !startsWithJSON(tail) {
		word, remaining := nextWord(tail)
		n, err := strconv.ParseInt(word, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid limit: %w", err)
		}
		if n > 0 {
			limit = n
		}
		tail = strings.TrimSpace(remaining)
	}

	// Optional sort JSON (object of field->1/-1).
	if startsWithJSON(tail) {
		jsonArg, _, err := extractJSONArg(tail)
		if err != nil {
			return nil, fmt.Errorf("invalid sort json: %w", err)
		}
		if err := bson.UnmarshalExtJSON([]byte(jsonArg), true, &sort); err != nil {
			return nil, fmt.Errorf("invalid sort json: %w", err)
		}
	}

	if limit > mongoMaxLimit {
		limit = mongoMaxLimit
	}

	findOpts := options.Find().SetLimit(limit)
	if len(sort) > 0 {
		findOpts.SetSort(sort)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	cur, err := d.db.Collection(collection).Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docsToQueryResult(docs), nil
}

func (d *MongoDB) runAggregate(rest string) (*QueryResult, error) {
	collection, tail := nextWord(rest)
	if collection == "" {
		return nil, fmt.Errorf("usage: aggregate <collection> <pipeline-json-array>")
	}
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return nil, fmt.Errorf("missing pipeline json")
	}

	pipelineJSON, _, err := extractJSONArg(tail)
	if err != nil {
		return nil, err
	}

	var pipeline []bson.D
	if err := bson.UnmarshalExtJSON([]byte(pipelineJSON), true, &pipeline); err != nil {
		return nil, fmt.Errorf("invalid pipeline json: %w", err)
	}
	stages := make(mongo.Pipeline, 0, len(pipeline))
	for _, stage := range pipeline {
		stages = append(stages, stage)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	cur, err := d.db.Collection(collection).Aggregate(ctx, stages)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docsToQueryResult(docs), nil
}

func (d *MongoDB) runCount(rest string) (*QueryResult, error) {
	collection, tail := nextWord(rest)
	if collection == "" {
		return nil, fmt.Errorf("usage: count <collection> [filter-json]")
	}
	filter := bson.M{}
	tail = strings.TrimSpace(tail)
	if tail != "" {
		if !startsWithJSON(tail) {
			return nil, fmt.Errorf("invalid filter json")
		}
		jsonArg, _, err := extractJSONArg(tail)
		if err != nil {
			return nil, err
		}
		if err := bson.UnmarshalExtJSON([]byte(jsonArg), true, &filter); err != nil {
			return nil, fmt.Errorf("invalid filter json: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	n, err := d.db.Collection(collection).CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Columns: []string{"count"}, Rows: [][]string{{strconv.FormatInt(n, 10)}}}, nil
}

func (d *MongoDB) runInsert(rest string) (*QueryResult, error) {
	collection, tail := nextWord(rest)
	if collection == "" {
		return nil, fmt.Errorf("usage: insert <collection> <document-json>")
	}
	tail = strings.TrimSpace(tail)
	if !startsWithJSON(tail) {
		return nil, fmt.Errorf("missing document json")
	}
	jsonArg, _, err := extractJSONArg(tail)
	if err != nil {
		return nil, err
	}

	var doc bson.M
	if err := bson.UnmarshalExtJSON([]byte(jsonArg), true, &doc); err != nil {
		return nil, fmt.Errorf("invalid document json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	res, err := d.db.Collection(collection).InsertOne(ctx, doc)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Message: fmt.Sprintf("inserted _id=%s", formatMongoValue(res.InsertedID)), Affected: 1}, nil
}

func (d *MongoDB) runUpdate(rest string) (*QueryResult, error) {
	collection, tail := nextWord(rest)
	if collection == "" {
		return nil, fmt.Errorf("usage: update <collection> <filter-json> <update-json> [many]")
	}
	tail = strings.TrimSpace(tail)

	filterJSON, remaining, err := extractJSONArg(tail)
	if err != nil {
		return nil, fmt.Errorf("missing/invalid filter json: %w", err)
	}
	updateJSON, remaining, err := extractJSONArg(strings.TrimSpace(remaining))
	if err != nil {
		return nil, fmt.Errorf("missing/invalid update json: %w", err)
	}
	many := strings.EqualFold(strings.TrimSpace(remaining), "many")

	var filter bson.M
	if err := bson.UnmarshalExtJSON([]byte(filterJSON), true, &filter); err != nil {
		return nil, fmt.Errorf("invalid filter json: %w", err)
	}
	var update bson.M
	if err := bson.UnmarshalExtJSON([]byte(updateJSON), true, &update); err != nil {
		return nil, fmt.Errorf("invalid update json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	if many {
		res, err := d.db.Collection(collection).UpdateMany(ctx, filter, update)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Message: fmt.Sprintf("matched=%d modified=%d", res.MatchedCount, res.ModifiedCount), Affected: res.ModifiedCount}, nil
	}
	res, err := d.db.Collection(collection).UpdateOne(ctx, filter, update)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Message: fmt.Sprintf("matched=%d modified=%d", res.MatchedCount, res.ModifiedCount), Affected: res.ModifiedCount}, nil
}

func (d *MongoDB) runDelete(rest string) (*QueryResult, error) {
	collection, tail := nextWord(rest)
	if collection == "" {
		return nil, fmt.Errorf("usage: delete <collection> <filter-json> [many]")
	}
	tail = strings.TrimSpace(tail)

	filterJSON, remaining, err := extractJSONArg(tail)
	if err != nil {
		return nil, fmt.Errorf("missing/invalid filter json: %w", err)
	}
	many := strings.EqualFold(strings.TrimSpace(remaining), "many")

	var filter bson.M
	if err := bson.UnmarshalExtJSON([]byte(filterJSON), true, &filter); err != nil {
		return nil, fmt.Errorf("invalid filter json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	if many {
		res, err := d.db.Collection(collection).DeleteMany(ctx, filter)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Message: fmt.Sprintf("deleted=%d", res.DeletedCount), Affected: res.DeletedCount}, nil
	}
	res, err := d.db.Collection(collection).DeleteOne(ctx, filter)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Message: fmt.Sprintf("deleted=%d", res.DeletedCount), Affected: res.DeletedCount}, nil
}

func parseMongoDBName(uri string) (string, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return "", errors.New("mongodb dsn is empty")
	}

	schemeIdx := strings.Index(uri, "://")
	if schemeIdx == -1 {
		return "", errors.New("invalid mongodb dsn")
	}
	rest := uri[schemeIdx+3:]
	slash := strings.Index(rest, "/")
	if slash == -1 {
		return "", errors.New("mongodb dsn must include a database name")
	}
	path := rest[slash+1:]
	if q := strings.Index(path, "?"); q >= 0 {
		path = path[:q]
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("mongodb dsn must include a database name")
	}
	if strings.Contains(path, "/") {
		path = strings.Split(path, "/")[0]
	}
	if path == "" {
		return "", errors.New("mongodb dsn must include a database name")
	}
	return path, nil
}

func nextWord(s string) (word, rest string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	idx := strings.IndexAny(s, " \t\n")
	if idx == -1 {
		return s, ""
	}
	return s[:idx], strings.TrimSpace(s[idx+1:])
}

func startsWithJSON(s string) bool {
	s = strings.TrimSpace(s)
	return strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[")
}

func extractJSONArg(s string) (jsonArg, rest string, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", errors.New("empty argument")
	}
	start := rune(s[0])
	var end rune
	switch start {
	case '{':
		end = '}'
	case '[':
		end = ']'
	default:
		return "", "", errors.New("expected json object or array")
	}

	depth := 0
	inString := false
	escaped := false
	for i, r := range s {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == '"' {
				inString = false
			}
			continue
		}

		if r == '"' {
			inString = true
			continue
		}
		if r == start {
			depth++
		}
		if r == end {
			depth--
			if depth == 0 {
				return s[:i+1], strings.TrimSpace(s[i+1:]), nil
			}
		}
	}
	return "", "", errors.New("unterminated json argument")
}

func docsToQueryResult(docs []bson.M) *QueryResult {
	if len(docs) == 0 {
		return &QueryResult{Columns: []string{"result"}, Rows: [][]string{}}
	}

	keySet := map[string]struct{}{}
	for _, doc := range docs {
		for k := range doc {
			keySet[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if contains(keys, "_id") {
		keys = append([]string{"_id"}, remove(keys, "_id")...)
	}

	rows := make([][]string, 0, len(docs))
	for _, doc := range docs {
		row := make([]string, len(keys))
		for i, k := range keys {
			row[i] = formatMongoValue(doc[k])
		}
		rows = append(rows, row)
	}

	return &QueryResult{Columns: keys, Rows: rows}
}

func mongoTypeName(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "bool"
	case int, int8, int16, int32, int64:
		return "int"
	case uint, uint8, uint16, uint32, uint64:
		return "uint"
	case float32, float64:
		return "float"
	case primitive.ObjectID:
		return "objectId"
	case primitive.DateTime, time.Time:
		return "date"
	case primitive.Decimal128:
		return "decimal"
	case primitive.Binary:
		return "binary"
	case []interface{}:
		return "array"
	case map[string]interface{}, bson.M:
		return "object"
	case bson.D:
		return "document"
	case primitive.Null:
		return "null"
	default:
		_ = t
		return fmt.Sprintf("%T", v)
	}
}

func formatMongoValue(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case primitive.ObjectID:
		return t.Hex()
	case primitive.DateTime:
		return t.Time().Format(time.RFC3339)
	case time.Time:
		return t.Format(time.RFC3339)
	case primitive.D:
		m := t.Map()
		b, err := bson.MarshalExtJSON(m, false, false)
		if err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", m)
	case bson.M, []interface{}:
		b, err := bson.MarshalExtJSON(t, false, false)
		if err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func remove(items []string, target string) []string {
	result := make([]string, 0, len(items))
	for _, item := range items {
		if item != target {
			result = append(result, item)
		}
	}
	return result
}

// parseShellQuery converts db.collection.method(args) to the internal query format.
// Returns the internal query string and true on success.
func parseShellQuery(query string) (string, bool) {
	q := strings.TrimSpace(query)
	if !strings.HasPrefix(q, "db.") {
		return "", false
	}
	rest := q[3:]

	dotIdx := strings.Index(rest, ".")
	if dotIdx < 0 {
		return "", false
	}
	collection := rest[:dotIdx]
	rest = rest[dotIdx+1:]

	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		return "", false
	}
	method := strings.ToLower(rest[:parenIdx])
	rest = rest[parenIdx+1:]

	argsStr, ok := extractShellArgs(rest)
	if !ok {
		return "", false
	}

	args := splitShellArgs(argsStr)

	arg0 := ""
	if len(args) > 0 {
		arg0 = strings.TrimSpace(args[0])
	}
	arg1 := ""
	if len(args) > 1 {
		arg1 = strings.TrimSpace(args[1])
	}
	if arg0 == "" {
		arg0 = "{}"
	}

	switch method {
	case "find":
		return "find " + collection + " " + arg0, true
	case "findone":
		return "find " + collection + " " + arg0 + " 1", true
	case "aggregate":
		if arg0 == "{}" {
			arg0 = "[]"
		}
		return "aggregate " + collection + " " + arg0, true
	case "updateone":
		if arg1 == "" {
			return "", false
		}
		return "update " + collection + " " + arg0 + " " + arg1, true
	case "updatemany":
		if arg1 == "" {
			return "", false
		}
		return "update " + collection + " " + arg0 + " " + arg1 + " many", true
	case "insertone":
		return "insert " + collection + " " + arg0, true
	case "insertmany":
		// treat as multiple inserts — just run the first doc or return unsupported
		return "insert " + collection + " " + arg0, true
	case "deleteone":
		return "delete " + collection + " " + arg0, true
	case "deletemany":
		return "delete " + collection + " " + arg0 + " many", true
	case "countdocuments", "count":
		return "count " + collection + " " + arg0, true
	}
	return "", false
}

// extractShellArgs extracts the content inside the outermost () of a method call.
// Input starts immediately after the opening '('.
func extractShellArgs(s string) (string, bool) {
	depth := 1
	inStr := false
	escape := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
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
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
			if depth == 0 {
				return s[:i], true
			}
		}
	}
	return strings.TrimRight(s, ")"), true // lenient: unclosed paren
}

// splitShellArgs splits top-level comma-separated args (not inside {}, [], ()).
func splitShellArgs(s string) []string {
	var args []string
	depth := 0
	inStr := false
	escape := false
	start := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
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
			depth--
		case ',':
			if depth == 0 {
				args = append(args, s[start:i])
				start = i + 1
			}
		}
	}
	args = append(args, s[start:])
	return args
}
