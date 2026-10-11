package orm

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
)

// QdrantStore runs the typed builder against a Qdrant vector database, over
// its REST API — no client library dependency. Collections are named after
// the table; payload fields are the columns; an orm.Vector column becomes
// the collection's (unnamed) vector, and Nearest ordering becomes a search.
//
//	orm.OpenQdrant("qdrant", "http://localhost:6333")
//	s.EnsureCollection(ctx, "docs", 384, orm.Cosine)
//
//	//orm:table docs connection=qdrant key_type=uuid
//	type Doc struct {
//		orm.Model
//		ID    string         `db:"id"`
//		Title string         `db:"title"`
//		Vec   orm.Vector[orm.D384] `db:"vec"`
//	}
//	docs, err := Docs.Query().OrderBy(orm.Nearest(Docs.Vec, q, orm.Cosine)).Limit(10).Get()
type QdrantStore struct {
	base string
	http *http.Client
}

// OpenQdrant connects to a Qdrant REST endpoint and registers the
// connection. A document's key column (the model's primary key, or a row
// query's WithKey) is its point id: a UUID or an unsigned integer.
func OpenQdrant(name, baseURL string) (*QdrantStore, error) {
	s := &QdrantStore{
		base: strings.TrimSuffix(baseURL, "/"),
		http: &http.Client{Timeout: 30 * time.Second},
	}
	AddDocStore(name, s)
	return s, nil
}

// qdrantHTTPError is a non-2xx REST response, kept typed so callers match
// on the status instead of the message text.
type qdrantHTTPError struct {
	method string
	path   string
	status int
	body   string
}

func (e *qdrantHTTPError) Error() string {
	return fmt.Sprintf("orm: qdrant %s %s: %d: %s", e.method, e.path, e.status, e.body)
}

// qdrantStatus reports err's HTTP status when err is a Qdrant REST error.
func qdrantStatus(err error) (int, bool) {
	if e, ok := errors.AsType[*qdrantHTTPError](err); ok {
		return e.status, true
	}
	return 0, false
}

var qdrantDistances = map[VectorMetric]string{L2: "Euclid", Cosine: "Cosine", InnerProduct: "Dot"}

// EnsureCollection creates the collection if it is missing, with the
// given vector size and distance metric. An existing collection must have
// that size and metric. Migrations declare the same through the schema
// package (a Vector column plus HnswIndex for the metric).
func (s *QdrantStore) EnsureCollection(ctx context.Context, table string, dim int, metric VectorMetric) error {
	want := DocVector{Size: dim, Metric: metric}
	err := s.CreateCollection(ctx, Collection{Name: table, Vector: &want})
	if status, ok := qdrantStatus(err); !ok || status != http.StatusConflict {
		return err
	}
	have, err := s.DescribeCollection(ctx, table)
	if err != nil {
		return err
	}
	if have.Vector == nil || have.Vector.Size != dim || have.Vector.Metric != metric {
		return fmt.Errorf("orm: qdrant collection %s exists with %s, not %d-dimension %s vectors",
			table, describeVector(have.Vector), dim, qdrantDistances[metric])
	}
	return nil
}

func describeVector(v *DocVector) string {
	if v == nil {
		return "no vectors"
	}
	return fmt.Sprintf("%d-dimension %s vectors", v.Size, qdrantDistances[v.Metric])
}

func (s *QdrantStore) Engine() string { return "qdrant" }

func (s *QdrantStore) Collections(ctx context.Context) ([]string, error) {
	var res qdrantResult[struct {
		Collections []struct {
			Name string `json:"name"`
		} `json:"collections"`
	}]
	if err := s.do(ctx, http.MethodGet, "/collections", nil, &res); err != nil {
		return nil, err
	}
	names := make([]string, len(res.Result.Collections))
	for i, c := range res.Result.Collections {
		names[i] = c.Name
	}
	slices.Sort(names)
	return names, nil
}

// CreateCollection creates the collection — with c's vector, or none —
// and a payload index per index. Qdrant payloads are schemaless, so the
// fields themselves need no declaration.
func (s *QdrantStore) CreateCollection(ctx context.Context, c Collection) error {
	vectors := map[string]any{}
	if c.Vector != nil {
		vectors = map[string]any{"size": c.Vector.Size, "distance": qdrantDistances[c.Vector.Metric]}
	}
	if err := s.do(ctx, http.MethodPut, collectionPath(c.Name, ""), map[string]any{"vectors": vectors}, nil); err != nil {
		return err
	}
	for _, idx := range c.Indexes {
		if err := s.CreateIndex(ctx, c.Name, idx); err != nil {
			return err
		}
	}
	return nil
}

// DescribeCollection reads the vector and the payload indexes; only
// indexed fields are known, since payloads are schemaless.
func (s *QdrantStore) DescribeCollection(ctx context.Context, name string) (Collection, error) {
	var res qdrantResult[struct {
		Config struct {
			Params struct {
				Vectors struct {
					Size     int    `json:"size"`
					Distance string `json:"distance"`
				} `json:"vectors"`
			} `json:"params"`
		} `json:"config"`
		PayloadSchema map[string]struct {
			DataType string `json:"data_type"`
		} `json:"payload_schema"`
	}]
	if err := s.do(ctx, http.MethodGet, collectionPath(name, ""), nil, &res); err != nil {
		return Collection{}, err
	}
	c := Collection{Name: name}
	if v := res.Result.Config.Params.Vectors; v.Size > 0 {
		metric, ok := metricOf(v.Distance)
		if !ok {
			return Collection{}, fmt.Errorf("orm: qdrant collection %s uses the unsupported distance %s", name, v.Distance)
		}
		c.Vector = &DocVector{Size: v.Size, Metric: metric}
	}
	for _, field := range sortedKeys(res.Result.PayloadSchema) {
		dataType := res.Result.PayloadSchema[field].DataType
		f := DocField{Name: field, Kind: qdrantKinds[dataType], Nullable: true}
		c.Fields = append(c.Fields, f)
		c.Indexes = append(c.Indexes, DocIndex{Name: field, Fields: []DocField{f}, Text: dataType == "text"})
	}
	return c, nil
}

func metricOf(distance string) (VectorMetric, bool) {
	for m, d := range qdrantDistances {
		if d == distance {
			return m, true
		}
	}
	return 0, false
}

// qdrantKinds maps payload index types to field kinds; qdrantSchemas is
// the reverse for the kinds Qdrant can index.
var (
	qdrantKinds = map[string]FieldKind{
		"keyword": KindString, "text": KindString, "uuid": KindString, "integer": KindInt,
		"float": KindFloat, "bool": KindBool, "datetime": KindTime,
	}
	qdrantSchemas = map[FieldKind]string{
		KindString: "keyword", KindInt: "integer", KindFloat: "float", KindBool: "bool", KindTime: "datetime",
	}
)

func (s *QdrantStore) RenameCollection(context.Context, string, string) error {
	return &ErrNotTranslatable{Reason: "renaming a Qdrant collection (Qdrant has no rename; create the new one and copy the points)"}
}

// SetFields accepts any fields: Qdrant payloads are schemaless.
func (s *QdrantStore) SetFields(context.Context, string, []DocField) error { return nil }

func (s *QdrantStore) DropFields(ctx context.Context, collection string, fields []string) error {
	return s.do(ctx, http.MethodPost, collectionPath(collection, "/points/payload/delete?wait=true"),
		map[string]any{"keys": fields, "filter": map[string]any{}}, nil)
}

func (s *QdrantStore) RenameField(context.Context, string, string, string) error {
	return &ErrNotTranslatable{Reason: "renaming a Qdrant payload field (Qdrant cannot move values between keys)"}
}

// CreateIndex creates a payload index on the index's single field, typed
// by the field's kind (text for a full text index). Qdrant has no unique
// or multi-field indexes.
func (s *QdrantStore) CreateIndex(ctx context.Context, collection string, idx DocIndex) error {
	switch {
	case idx.Primary && len(idx.Fields) == 1:
		return nil
	case idx.Primary:
		return &ErrNotTranslatable{Reason: "a multi-column primary key on Qdrant (the point id is one column)"}
	case idx.Unique:
		return &ErrNotTranslatable{Reason: "unique indexes on Qdrant"}
	case len(idx.Fields) != 1:
		return &ErrNotTranslatable{Reason: "multi-field indexes on Qdrant (index each field)"}
	}
	f := idx.Fields[0]
	schema, ok := qdrantSchemas[f.Kind]
	if idx.Text {
		schema, ok = "text", f.Kind == KindString
	}
	if !ok {
		return &ErrNotTranslatable{Reason: fmt.Sprintf("a payload index on %s, a field of kind %s; declare it with its type in the same blueprint, e.g. t.Integer(%q).Index()", f.Name, f.Kind, f.Name)}
	}
	return s.do(ctx, http.MethodPut, collectionPath(collection, "/index?wait=true"),
		map[string]any{"field_name": f.Name, "field_schema": schema}, nil)
}

func (s *QdrantStore) DropIndex(ctx context.Context, collection string, idx DocIndex) error {
	if len(idx.Fields) != 1 {
		return &ErrNotTranslatable{Reason: "multi-field indexes on Qdrant"}
	}
	return s.do(ctx, http.MethodDelete, collectionPath(collection, "/index/"+url.PathEscape(idx.Fields[0].Name)+"?wait=true"), nil, nil)
}

// collectionPath is the REST path of a collection, with suffix appended.
func collectionPath(table, suffix string) string {
	return "/collections/" + url.PathEscape(table) + suffix
}

// DropCollection removes a collection (dropIfExists in a migration).
func (s *QdrantStore) DropCollection(ctx context.Context, table string) error {
	err := s.do(ctx, http.MethodDelete, collectionPath(table, ""), nil, nil)
	if status, ok := qdrantStatus(err); ok && status == http.StatusNotFound {
		return nil
	}
	return err
}

func (s *QdrantStore) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(res.Body)
		return &qdrantHTTPError{method: method, path: path, status: res.StatusCode, body: string(b)}
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(res.Body)
	dec.UseNumber()
	return dec.Decode(out)
}

// jsonNumbers replaces the json.Numbers a UseNumber decode leaves with
// int64 where the number is an integer that fits and float64 where it
// does not, so integers beyond 2^53 keep every digit.
func jsonNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
	case map[string]any:
		for k, e := range x {
			x[k] = jsonNumbers(e)
		}
	case []any:
		for i, e := range x {
			x[i] = jsonNumbers(e)
		}
	}
	return v
}

type qdrantResult[T any] struct {
	Result T `json:"result"`
}

func (s *QdrantStore) FindDocs(ctx context.Context, p Plan) ([]map[string]any, error) {
	table, key := p.Table, p.Key
	if key == "" {
		return nil, fmt.Errorf("orm: qdrant find on %s: the plan names no key column", table)
	}
	var after any
	if p.Vector == nil && keyOrdered(p, key) {
		p.Orders = nil
		p.Where, after = liftKeyset(p.Where, key)
	}
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return nil, err
	}
	if p.Vector != nil {
		return s.search(ctx, p, filter)
	}
	if len(p.Orders) > 1 {
		return nil, &ErrNotTranslatable{Reason: "more than one order (Qdrant orders by payload field or by vector)"}
	}
	docs, err := s.scroll(ctx, p, filter, after)
	if err != nil {
		return nil, err
	}
	if len(p.Columns) > 0 {
		keep := map[string]bool{}
		for _, c := range p.Columns {
			keep[c] = true
		}
		for _, d := range docs {
			for k := range d {
				if !keep[k] {
					delete(d, k)
				}
			}
		}
	}
	return docs, nil
}

type qdrantPoint struct {
	ID      any            `json:"id"`
	Payload map[string]any `json:"payload"`
}

func pointsToDocs(points []qdrantPoint, key string) []map[string]any {
	docs := make([]map[string]any, len(points))
	for i, pt := range points {
		doc := pt.Payload
		if doc == nil {
			doc = map[string]any{}
		}
		jsonNumbers(doc)
		if _, ok := doc[key]; !ok {
			doc[key] = jsonNumbers(pt.ID)
		}
		docs[i] = doc
	}
	return docs
}

// keyOrdered reports whether p reads in point-id order: unordered, or
// ordered by the key ascending. Qdrant scrolls in that order natively,
// with no payload index.
func keyOrdered(p Plan, key string) bool {
	return len(p.Orders) == 0 || len(p.Orders) == 1 && p.Orders[0].Column == key && !p.Orders[0].Desc
}

// liftKeyset takes a top-level "key > x" condition (keyset pagination,
// ChunkByID) out of where: Qdrant filters cannot compare point ids, but a
// scroll resumes from one.
func liftKeyset(where Node, key string) (Node, any) {
	isAfter := func(n Node) (any, bool) {
		f, ok := n.(Field)
		return f.Value, ok && f.Column == key && f.Op == ">"
	}
	if v, ok := isAfter(where); ok {
		return nil, v
	}
	c, ok := where.(Composite)
	if !ok || c.Or {
		return where, nil
	}
	for i, part := range c.Parts {
		if v, ok := isAfter(part); ok {
			return Composite{Parts: slices.Delete(slices.Clone(c.Parts), i, i+1)}, v
		}
	}
	return where, nil
}

// search ranks the matching points by distance from p.Vector. Without a
// limit it pages through every match, as an unlimited SQL KNN query
// returns every row.
func (s *QdrantStore) search(ctx context.Context, p Plan, filter map[string]any) ([]map[string]any, error) {
	page := p.Limit
	if page == 0 {
		page = scrollBatch
	}
	body := map[string]any{"vector": p.Vector.Floats, "limit": page, "with_payload": true, "filter": filter}
	var docs []map[string]any
	for offset := p.Offset; ; offset += page {
		body["offset"] = offset
		var res qdrantResult[[]qdrantPoint]
		if err := s.do(ctx, http.MethodPost, collectionPath(p.Table, "/points/search"), body, &res); err != nil {
			return nil, err
		}
		docs = append(docs, pointsToDocs(res.Result, p.Key)...)
		if p.Limit > 0 || len(res.Result) < page {
			return docs, nil
		}
	}
}

// scrollBatch is how many points one scroll request fetches.
const scrollBatch = 256

// scroll reads the points matching filter in p's order, after the point
// id after when it is set, skipping p.Offset and stopping after p.Limit.
// Qdrant's scroll offset is a point id to resume from (inclusive), not a
// count, so the skip happens here; with order_by Qdrant returns no resume
// point, so the read is sized by a count instead.
func (s *QdrantStore) scroll(ctx context.Context, p Plan, filter map[string]any, after any) ([]map[string]any, error) {
	table, key := p.Table, p.Key
	want := 0
	if p.Limit > 0 {
		want = p.Offset + p.Limit
	}
	body := map[string]any{"with_payload": true, "with_vector": false, "filter": filter}
	if len(p.Orders) == 1 {
		if p.Orders[0].Desc {
			return nil, &ErrNotTranslatable{Reason: "descending order (Qdrant order_by is ascending; store a negated field to sort descending)"}
		}
		if p.Orders[0].Nulls == NullsFirst {
			return nil, &ErrNotTranslatable{Reason: "NULLs first (Qdrant sorts points without " + p.Orders[0].Column + " last)"}
		}
		body["order_by"] = p.Orders[0].Column
		if want == 0 {
			n, err := s.CountDocs(ctx, p)
			if err != nil || n == 0 {
				return nil, err
			}
			want = int(n)
		}
	}
	skipAfter := after != nil
	if skipAfter {
		body["offset"] = after
		if want > 0 {
			want++
		}
	}
	var docs []map[string]any
	for want == 0 || len(docs) < want {
		batch := scrollBatch
		if want > 0 {
			batch = min(batch, want-len(docs))
		}
		if len(p.Orders) == 1 {
			batch = want
		}
		body["limit"] = batch
		var res qdrantResult[struct {
			Points []qdrantPoint `json:"points"`
			Next   any           `json:"next_page_offset"`
		}]
		if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points/scroll"), body, &res); err != nil {
			return nil, err
		}
		docs = append(docs, pointsToDocs(res.Result.Points, key)...)
		if res.Result.Next == nil || len(p.Orders) == 1 || len(res.Result.Points) == 0 {
			break
		}
		body["offset"] = res.Result.Next
	}
	if len(p.Orders) == 1 && len(docs) < want {
		rest, err := s.missingLast(ctx, p, want-len(docs))
		if err != nil {
			return nil, err
		}
		docs = append(docs, rest...)
	}
	if skipAfter && len(docs) > 0 && fmt.Sprint(docs[0][key]) == fmt.Sprint(after) {
		docs = docs[1:]
	}
	docs = docs[min(p.Offset, len(docs)):]
	if p.Limit > 0 && len(docs) > p.Limit {
		docs = docs[:p.Limit]
	}
	return docs, nil
}

// missingLast reads up to n matching points that lack p's order field,
// which Qdrant's order_by leaves out; they sort last, as NULLs do in
// ascending Postgres order.
func (s *QdrantStore) missingLast(ctx context.Context, p Plan, n int) ([]map[string]any, error) {
	parts := []Node{NullTest{Column: p.Orders[0].Column}}
	if p.Where != nil {
		parts = append(parts, p.Where)
	}
	q := Plan{Table: p.Table, Key: p.Key, Where: Composite{Parts: parts}, Limit: n}
	filter, err := qdrantFilter(q.Where)
	if err != nil {
		return nil, err
	}
	return s.scroll(ctx, q, filter, nil)
}

// InsertDocs stores new points, refusing ids already taken: Qdrant's PUT
// would silently overwrite them. The check and the write are two requests,
// so a concurrent insert of the same id can still win.
func (s *QdrantStore) InsertDocs(ctx context.Context, table, key string, docs []map[string]any) error {
	points, ids, err := qdrantPoints(table, key, docs)
	if err != nil || len(points) == 0 {
		return err
	}
	taken, err := s.existingIDs(ctx, table, ids)
	if err != nil {
		return err
	}
	if len(taken) > 0 {
		return &DuplicateKeyError{Table: table, Key: key, Err: fmt.Errorf("point ids %v exist", slices.Sorted(maps.Keys(taken)))}
	}
	return s.do(ctx, http.MethodPut, collectionPath(table, "/points?wait=true"), map[string]any{"points": points}, nil)
}

// UpsertDocs inserts new points whole and, for points whose id exists,
// sets only the update columns (and the vector, when it is one of them),
// all in one batch request. Qdrant can only match on the point id.
func (s *QdrantStore) UpsertDocs(ctx context.Context, table, key string, docs []map[string]any, uniqueBy, update []string) error {
	if len(uniqueBy) != 1 || uniqueBy[0] != key {
		return &ErrNotTranslatable{Reason: fmt.Sprintf("upserting on Qdrant by %v (Qdrant matches only the point id %q)", uniqueBy, key)}
	}
	points, ids, err := qdrantPoints(table, key, docs)
	if err != nil || len(points) == 0 {
		return err
	}
	taken, err := s.existingIDs(ctx, table, ids)
	if err != nil {
		return err
	}
	var fresh []map[string]any
	var ops []map[string]any
	for i, pt := range points {
		if !taken[fmt.Sprint(ids[i])] {
			fresh = append(fresh, pt)
			continue
		}
		set := map[string]any{}
		for _, c := range update {
			if v, ok := docs[i][c]; ok && c != key {
				set[c] = v
			}
		}
		changes, err := qdrantChanges(set, map[string]any{"points": []any{ids[i]}})
		if err != nil {
			return fmt.Errorf("orm: qdrant upsert into %s: %w", table, err)
		}
		ops = append(ops, changes...)
		if vec, ok := pt["vector"].([]float64); ok && slices.ContainsFunc(update, func(c string) bool { _, isVec := vectorOf(docs[i][c]); return isVec }) {
			ops = append(ops, map[string]any{"update_vectors": map[string]any{"points": []any{map[string]any{"id": ids[i], "vector": vec}}}})
		}
	}
	if len(fresh) > 0 {
		ops = append([]map[string]any{{"upsert": map[string]any{"points": fresh}}}, ops...)
	}
	return s.batch(ctx, table, ops)
}

// qdrantPoints converts documents to points, keyed by their key column,
// rejecting a missing or repeated key.
func qdrantPoints(table, key string, docs []map[string]any) ([]map[string]any, []any, error) {
	points := make([]map[string]any, 0, len(docs))
	ids := make([]any, 0, len(docs))
	seen := make(map[string]bool, len(docs))
	for _, doc := range docs {
		id, ok := doc[key]
		if !ok || isZero(id) {
			return nil, nil, fmt.Errorf("orm: qdrant insert into %s: every point needs a key in %q", table, key)
		}
		if err := qdrantPointID(id); err != nil {
			return nil, nil, fmt.Errorf("orm: qdrant insert into %s: key %s: %w", table, key, err)
		}
		if seen[fmt.Sprint(id)] {
			return nil, nil, &DuplicateKeyError{Table: table, Key: key, Err: fmt.Errorf("point id %v repeats in one write", id)}
		}
		seen[fmt.Sprint(id)] = true
		payload := make(map[string]any, len(doc))
		var vector []float64
		for k, v := range doc {
			// Every column lands in the payload, the key and the vector
			// included: filters match payload fields, not point metadata, so
			// documents must be self-contained to be queryable after a read.
			jv, err := jsonValue(v)
			if err != nil {
				return nil, nil, fmt.Errorf("orm: qdrant insert into %s: field %s: %w", table, k, err)
			}
			payload[k] = jv
			if f, ok := vectorOf(v); ok {
				vector = f
			}
		}
		pt := map[string]any{"id": id, "payload": payload, "vector": map[string]any{}}
		if vector != nil {
			pt["vector"] = vector
		}
		points = append(points, pt)
		ids = append(ids, id)
	}
	return points, ids, nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// qdrantPointID refuses a key Qdrant cannot use as a point id: those are
// UUIDs or unsigned integers, so a ULID or other text key cannot be one.
func qdrantPointID(id any) error {
	switch v := id.(type) {
	case string:
		if uuidPattern.MatchString(v) {
			return nil
		}
	case int, int8, int16, int32, int64:
		if reflect.ValueOf(v).Int() >= 0 {
			return nil
		}
	case uint, uint8, uint16, uint32, uint64:
		return nil
	}
	return fmt.Errorf("%v is not a Qdrant point id (a UUID or an unsigned integer); use key_type=uuid", id)
}

// existingIDs reports which of ids are taken, as fmt.Sprint keys.
func (s *QdrantStore) existingIDs(ctx context.Context, table string, ids []any) (map[string]bool, error) {
	var res qdrantResult[[]qdrantPoint]
	body := map[string]any{"ids": ids, "with_payload": false, "with_vector": false}
	if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points"), body, &res); err != nil {
		return nil, err
	}
	taken := make(map[string]bool, len(res.Result))
	for _, pt := range res.Result {
		taken[fmt.Sprint(jsonNumbers(pt.ID))] = true
	}
	return taken, nil
}

// qdrantChanges are the batch operations setting the non-nil values of set
// and deleting the keys set clears, on the points selector picks.
func qdrantChanges(set map[string]any, selector map[string]any) ([]map[string]any, error) {
	payload := map[string]any{}
	var cleared []string
	for _, k := range sortedKeys(set) {
		jv, err := jsonValue(set[k])
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", k, err)
		}
		if jv == nil {
			cleared = append(cleared, k)
		} else {
			payload[k] = jv
		}
	}
	var ops []map[string]any
	if len(payload) > 0 {
		op := maps.Clone(selector)
		op["payload"] = payload
		ops = append(ops, map[string]any{"set_payload": op})
	}
	if len(cleared) > 0 {
		op := maps.Clone(selector)
		op["keys"] = cleared
		ops = append(ops, map[string]any{"delete_payload": op})
	}
	return ops, nil
}

func (s *QdrantStore) batch(ctx context.Context, table string, ops []map[string]any) error {
	if len(ops) == 0 {
		return nil
	}
	return s.do(ctx, http.MethodPost, collectionPath(table, "/points/batch?wait=true"), map[string]any{"operations": ops}, nil)
}

// UpdateDocs sets payload fields on the matching points; a vector value
// also replaces those points' vector, which search reads instead of the
// payload copy.
func (s *QdrantStore) UpdateDocs(ctx context.Context, p Plan, u DocUpdate) (int64, error) {
	table := p.Table
	if p.Vector != nil {
		return 0, &ErrNotTranslatable{Reason: "updating by vector search"}
	}
	if len(u.Inc) > 0 {
		return 0, &ErrNotTranslatable{Reason: "increments on Qdrant, which has no atomic payload arithmetic"}
	}
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return 0, err
	}
	n, err := s.CountDocs(ctx, p)
	if err != nil {
		return 0, err
	}
	ops, err := qdrantChanges(u.Set, map[string]any{"filter": filter})
	if err != nil {
		return 0, fmt.Errorf("orm: qdrant update on %s: %w", table, err)
	}
	for _, k := range sortedKeys(u.Set) {
		vec, ok := vectorOf(u.Set[k])
		if !ok {
			continue
		}
		docs, err := s.FindDocs(ctx, Plan{Table: table, Key: p.Key, Columns: []string{p.Key}, Where: p.Where})
		if err != nil {
			return 0, err
		}
		points := make([]any, len(docs))
		for i, d := range docs {
			points[i] = map[string]any{"id": d[p.Key], "vector": vec}
		}
		if len(points) > 0 {
			ops = append([]map[string]any{{"update_vectors": map[string]any{"points": points}}}, ops...)
		}
	}
	if err := s.batch(ctx, table, ops); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *QdrantStore) DeleteDocs(ctx context.Context, p Plan) (int64, error) {
	if p.Vector != nil {
		return 0, &ErrNotTranslatable{Reason: "deleting by vector search"}
	}
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return 0, err
	}
	n, err := s.CountDocs(ctx, p)
	if err != nil {
		return 0, err
	}
	body := map[string]any{"filter": filter}
	if err := s.do(ctx, http.MethodPost, collectionPath(p.Table, "/points/delete?wait=true"), body, nil); err != nil {
		return 0, err
	}
	return n, nil
}

// AggregateDocs is refused: Qdrant has no server-side aggregation, and
// summing by scrolling every point would hide an unbounded scan.
func (s *QdrantStore) AggregateDocs(context.Context, Plan, Aggregate, string) (any, error) {
	return nil, &ErrNotTranslatable{Reason: "aggregates on Qdrant (it has no server-side aggregation)"}
}

func (s *QdrantStore) CountDocs(ctx context.Context, p Plan) (int64, error) {
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return 0, err
	}
	body := map[string]any{"exact": true, "filter": filter}
	var res qdrantResult[struct {
		Count int64 `json:"count"`
	}]
	if err := s.do(ctx, http.MethodPost, collectionPath(p.Table, "/points/count"), body, &res); err != nil {
		return 0, err
	}
	return res.Result.Count, nil
}

// qdrantTime is fixed-width RFC 3339 in UTC, so equal instants are equal
// strings and string order is time order.
const qdrantTime = "2006-01-02T15:04:05.000000000Z"

// jsonValue converts a value to its JSON representation for payloads. A
// driver.Valuer's error is returned, never papered over: Encrypted fails
// that way without a key, and storing the value as-is would write it in
// plain text.
func jsonValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, int, int32, int64, uint, uint32, uint64, float64, []any, map[string]any:
		return v, nil
	case []byte:
		return nil, &ErrNotTranslatable{Reason: "binary values on Qdrant (payloads are JSON; store them as text)"}
	case time.Time:
		return x.UTC().Format(qdrantTime), nil
	case *time.Time:
		if x == nil {
			return nil, nil
		}
		return x.UTC().Format(qdrantTime), nil
	case driver.Valuer:
		raw, err := x.Value()
		if err != nil {
			return nil, fmt.Errorf("converting %T: %w", v, err)
		}
		return jsonValue(raw)
	}
	return v, nil
}

// qdrantFilter compiles a plan condition to a Qdrant filter object; the
// shapes Qdrant cannot express (LIKE) are refused.
func qdrantFilter(n Node) (map[string]any, error) {
	if n == nil {
		return map[string]any{}, nil
	}
	return qdrantCompile(lowerNegations(n, false))
}

// qdrantNever matches no point.
func qdrantNever() map[string]any {
	return map[string]any{"must": []any{map[string]any{"has_id": []any{}}}}
}

// qdrantExcluding is a filter matching neither cond nor points whose
// column is null or missing, which SQL's negations never match.
func qdrantExcluding(column string, cond map[string]any) map[string]any {
	return map[string]any{"must_not": []any{cond, map[string]any{"is_empty": map[string]any{"key": column}}}}
}

// qdrantCompile compiles a negation-lowered condition into a filter;
// compound conditions nest, since a filter is itself a Qdrant condition.
func qdrantCompile(n Node) (map[string]any, error) {
	must := func(cond map[string]any) map[string]any { return map[string]any{"must": []any{cond}} }
	switch c := n.(type) {
	case Field:
		v, err := jsonValue(c.Value)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return qdrantNever(), nil
		}
		switch c.Op {
		case "=":
			return must(matchCond(c.Column, v)), nil
		case "<>", "!=":
			return qdrantExcluding(c.Column, matchCond(c.Column, v)), nil
		case ">", ">=", "<", "<=":
			return must(map[string]any{"key": c.Column, "range": map[string]any{rangeOp(c.Op): v}}), nil
		}
		return nil, &ErrNotTranslatable{Reason: "operator " + c.Op + " on Qdrant"}
	case List:
		if len(c.Values) == 0 {
			if c.Not {
				return map[string]any{}, nil
			}
			return qdrantNever(), nil
		}
		vals := make([]any, len(c.Values))
		for i, v := range c.Values {
			var err error
			if vals[i], err = jsonValue(v); err != nil {
				return nil, err
			}
		}
		cond := map[string]any{"key": c.Column, "match": map[string]any{"any": vals}}
		if c.Not {
			return qdrantExcluding(c.Column, cond), nil
		}
		return must(cond), nil
	case Range:
		lo, err := jsonValue(c.Lo)
		if err != nil {
			return nil, err
		}
		hi, err := jsonValue(c.Hi)
		if err != nil {
			return nil, err
		}
		cond := map[string]any{"key": c.Column, "range": map[string]any{"gte": lo, "lte": hi}}
		if c.Not {
			return qdrantExcluding(c.Column, cond), nil
		}
		return must(cond), nil
	case NullTest:
		empty := map[string]any{"is_empty": map[string]any{"key": c.Column}}
		if c.Not {
			return map[string]any{"must_not": []any{empty}}, nil
		}
		return must(empty), nil
	case Composite:
		parts := make([]any, 0, len(c.Parts))
		for _, p := range c.Parts {
			sub, err := qdrantCompile(p)
			if err != nil {
				return nil, err
			}
			if len(sub) == 0 {
				if c.Or {
					return map[string]any{}, nil
				}
				continue
			}
			parts = append(parts, sub)
		}
		switch {
		case len(parts) == 0 && c.Or:
			return qdrantNever(), nil
		case len(parts) == 0:
			return map[string]any{}, nil
		case c.Or:
			return map[string]any{"should": parts}, nil
		}
		return map[string]any{"must": parts}, nil
	case Negate:
		return nil, &ErrNotTranslatable{Reason: "negating this condition on Qdrant"}
	case notTranslatable:
		return nil, &ErrNotTranslatable{Reason: "conditions on " + c.reason + " on Qdrant"}
	}
	return nil, &ErrNotTranslatable{Reason: "this condition on Qdrant"}
}

func matchCond(column string, v any) map[string]any {
	return map[string]any{"key": column, "match": map[string]any{"value": v}}
}

func rangeOp(op string) string {
	return map[string]string{">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}[op]
}
