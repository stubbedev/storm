package orm

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
//		Vec   orm.Vector[384] `db:"vec"`
//	}
//	docs, err := Docs.Query().OrderBy(Docs.Vec.Nearest(q, orm.Cosine)).Limit(10).Get(ctx)
type QdrantStore struct {
	base  string
	http  *http.Client
	keyOf func(table string) string
}

// OpenQdrant connects to a Qdrant REST endpoint and registers the
// connection. Keys are UUID strings by default; use WithKey to map another
// column.
func OpenQdrant(name, baseURL string) (*QdrantStore, error) {
	s := &QdrantStore{
		base:  strings.TrimSuffix(baseURL, "/"),
		http:  &http.Client{Timeout: 30 * time.Second},
		keyOf: func(string) string { return "id" },
	}
	AddDocStore(name, s)
	return s, nil
}

// WithKey names the column whose value is the point id (default "id").
func (s *QdrantStore) WithKey(col string) *QdrantStore {
	s.keyOf = func(string) string { return col }
	return s
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
	case idx.Primary && len(idx.Fields) == 1 && idx.Fields[0].Name == s.key(collection):
		return nil
	case idx.Primary:
		return &ErrNotTranslatable{Reason: fmt.Sprintf("a primary key other than the point id %q on Qdrant", s.key(collection))}
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
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	return nil
}

type qdrantResult[T any] struct {
	Result T `json:"result"`
}

func (s *QdrantStore) key(table string) string { return s.keyOf(table) }

func (s *QdrantStore) FindDocs(ctx context.Context, table string, p Plan) ([]map[string]any, error) {
	var after any
	if p.Vector == nil && keyOrdered(p, s.key(table)) {
		p.Orders = nil
		p.Where, after = liftKeyset(p.Where, s.key(table))
	}
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return nil, err
	}
	if p.Vector != nil {
		limit := 10
		if p.Limit > 0 {
			limit = p.Limit
		}
		body := map[string]any{
			"vector":       p.Vector.Floats,
			"limit":        limit,
			"with_payload": true,
		}
		if len(filter) > 0 {
			body["filter"] = filter
		}
		if p.Offset > 0 {
			body["offset"] = p.Offset
		}
		var res qdrantResult[[]qdrantPoint]
		if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points/search"), body, &res); err != nil {
			return nil, err
		}
		return pointsToDocs(res.Result, s.key(table)), nil
	}
	if len(p.Orders) > 1 {
		return nil, &ErrNotTranslatable{Reason: "more than one order (Qdrant orders by payload field or by vector)"}
	}
	docs, err := s.scroll(ctx, table, p, filter, after)
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
		if _, ok := doc[key]; !ok {
			doc[key] = pt.ID
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

// scrollBatch is how many points one scroll request fetches.
const scrollBatch = 256

// scroll reads the points matching filter in p's order, after the point
// id after when it is set, skipping p.Offset and stopping after p.Limit.
// Qdrant's scroll offset is a point id to resume from (inclusive), not a
// count, so the skip happens here; with order_by Qdrant returns no resume
// point, so the read is sized by a count instead.
func (s *QdrantStore) scroll(ctx context.Context, table string, p Plan, filter map[string]any, after any) ([]map[string]any, error) {
	want := 0
	if p.Limit > 0 {
		want = p.Offset + p.Limit
	}
	body := map[string]any{"with_payload": true, "with_vector": false}
	if len(filter) > 0 {
		body["filter"] = filter
	}
	if len(p.Orders) == 1 {
		if p.Orders[0].Desc {
			return nil, &ErrNotTranslatable{Reason: "descending order (Qdrant order_by is ascending; store a negated field to sort descending)"}
		}
		body["order_by"] = p.Orders[0].Column
		if want == 0 {
			n, err := s.CountDocs(ctx, table, p)
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
		docs = append(docs, pointsToDocs(res.Result.Points, s.key(table))...)
		if res.Result.Next == nil || len(p.Orders) == 1 || len(res.Result.Points) == 0 {
			break
		}
		body["offset"] = res.Result.Next
	}
	if skipAfter && len(docs) > 0 && fmt.Sprint(docs[0][s.key(table)]) == fmt.Sprint(after) {
		docs = docs[1:]
	}
	docs = docs[min(p.Offset, len(docs)):]
	if p.Limit > 0 && len(docs) > p.Limit {
		docs = docs[:p.Limit]
	}
	return docs, nil
}

func (s *QdrantStore) InsertDocs(ctx context.Context, table string, docs []map[string]any) error {
	key := s.key(table)
	points := make([]map[string]any, 0, len(docs))
	for _, doc := range docs {
		id, ok := doc[key]
		if !ok || isZero(id) {
			return fmt.Errorf("orm: qdrant insert into %s: every point needs a key in %q", table, key)
		}
		payload := make(map[string]any, len(doc))
		var vector []float64
		for k, v := range doc {
			// Every column lands in the payload, the key and the vector
			// included: filters match payload fields, not point metadata, so
			// documents must be self-contained to be queryable after a read.
			jv, err := jsonValue(v)
			if err != nil {
				return fmt.Errorf("orm: qdrant insert into %s: field %s: %w", table, k, err)
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
	}
	if len(points) == 0 {
		return nil
	}
	return s.do(ctx, http.MethodPut, collectionPath(table, "/points?wait=true"),
		map[string]any{"points": points}, nil)
}

// UpsertDocs overwrites the matching points: the PUT points endpoint is
// idempotent on the point id, which is the document key.
func (s *QdrantStore) UpsertDocs(ctx context.Context, table string, docs []map[string]any, keys []string) error {
	return s.InsertDocs(ctx, table, docs)
}

func (s *QdrantStore) UpdateDocs(ctx context.Context, table string, p Plan, u DocUpdate) (int64, error) {
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
	n, err := s.CountDocs(ctx, table, p)
	if err != nil {
		return 0, err
	}
	payload := map[string]any{}
	var nils []string
	for _, k := range sortedKeys(u.Set) {
		jv, err := jsonValue(u.Set[k])
		if err != nil {
			return 0, fmt.Errorf("orm: qdrant update on %s: field %s: %w", table, k, err)
		}
		if jv == nil {
			nils = append(nils, k)
		} else {
			payload[k] = jv
		}
	}
	body := map[string]any{"payload": payload}
	if len(filter) > 0 {
		body["filter"] = filter
	}
	if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points/payload?wait=true"), body, nil); err != nil {
		return 0, err
	}
	for _, k := range nils {
		body := map[string]any{"keys": []string{k}}
		if len(filter) > 0 {
			body["filter"] = filter
		}
		if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points/payload/delete?wait=true"), body, nil); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (s *QdrantStore) DeleteDocs(ctx context.Context, table string, p Plan) (int64, error) {
	if p.Vector != nil {
		return 0, &ErrNotTranslatable{Reason: "deleting by vector search"}
	}
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return 0, err
	}
	n, err := s.CountDocs(ctx, table, p)
	if err != nil {
		return 0, err
	}
	body := map[string]any{}
	if len(filter) > 0 {
		body["filter"] = filter
	}
	if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points/delete?wait=true"), body, nil); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *QdrantStore) CountDocs(ctx context.Context, table string, p Plan) (int64, error) {
	filter, err := qdrantFilter(p.Where)
	if err != nil {
		return 0, err
	}
	body := map[string]any{"exact": true}
	if len(filter) > 0 {
		body["filter"] = filter
	}
	var res qdrantResult[struct {
		Count int64 `json:"count"`
	}]
	if err := s.do(ctx, http.MethodPost, collectionPath(table, "/points/count"), body, &res); err != nil {
		return 0, err
	}
	return res.Result.Count, nil
}

// jsonValue converts a value to its JSON representation for payloads. A
// driver.Valuer's error is returned, never papered over: Encrypted fails
// that way without a key, and storing the value as-is would write it in
// plain text.
func jsonValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, int, int32, int64, uint, uint32, uint64, float64, []any, map[string]any:
		return v, nil
	case time.Time:
		return x.Format(time.RFC3339Nano), nil
	case driver.Valuer:
		raw, err := x.Value()
		if err != nil {
			return nil, fmt.Errorf("converting %T: %w", v, err)
		}
		return jsonValue(raw)
	}
	return v, nil
}

// qdrantFilter compiles a plan condition to a Qdrant filter object. Each
// condition lands in must / must_not / should; the shapes Qdrant cannot
// express (LIKE, deep nesting) are refused.
func qdrantFilter(n Node) (map[string]any, error) {
	if n == nil {
		return nil, nil
	}
	conds, err := qdrantConds(n)
	if err != nil {
		return nil, err
	}
	buckets := map[string][]map[string]any{"must": {}, "must_not": {}, "should": {}}
	for _, c := range conds {
		buckets[c.bucket] = append(buckets[c.bucket], c.cond)
	}
	f := map[string]any{}
	for kind, list := range buckets {
		if len(list) > 0 {
			f[kind] = list
		}
	}
	return f, nil
}

type qdrantCond struct {
	bucket string
	cond   map[string]any
}

func qdrantConds(n Node) ([]qdrantCond, error) {
	switch c := n.(type) {
	case Field:
		v, err := jsonValue(c.Value)
		if err != nil {
			return nil, err
		}
		switch c.Op {
		case "=":
			return []qdrantCond{{"must", matchCond(c.Column, v)}}, nil
		case "<>", "!=":
			return []qdrantCond{{"must_not", matchCond(c.Column, v)}}, nil
		case ">", ">=", "<", "<=":
			return []qdrantCond{{"must", map[string]any{"key": c.Column, "range": map[string]any{rangeOp(c.Op): v}}}}, nil
		}
		return nil, &ErrNotTranslatable{Reason: "operator " + c.Op + " on Qdrant"}
	case List:
		vals := make([]any, len(c.Values))
		for i, v := range c.Values {
			var err error
			if vals[i], err = jsonValue(v); err != nil {
				return nil, err
			}
		}
		bucket := "must"
		if c.Not {
			bucket = "must_not"
		}
		return []qdrantCond{{bucket, map[string]any{"key": c.Column, "match": map[string]any{"any": vals}}}}, nil
	case Range:
		bucket := "must"
		if c.Not {
			bucket = "must_not"
		}
		lo, err := jsonValue(c.Lo)
		if err != nil {
			return nil, err
		}
		hi, err := jsonValue(c.Hi)
		if err != nil {
			return nil, err
		}
		return []qdrantCond{{bucket, map[string]any{"key": c.Column, "range": map[string]any{"gte": lo, "lte": hi}}}}, nil
	case NullTest:
		empty := map[string]any{"is_empty": map[string]any{"key": c.Column}}
		if c.Not {
			return []qdrantCond{{"must_not", empty}}, nil
		}
		return []qdrantCond{{"must", empty}}, nil
	case Composite:
		parts := make([]qdrantCond, 0)
		for _, p := range c.Parts {
			sub, err := qdrantConds(p)
			if err != nil {
				return nil, err
			}
			parts = append(parts, sub...)
		}
		if c.Or {
			// should: any single condition matching satisfies the filter.
			for i := range parts {
				parts[i].bucket = "should"
			}
			return parts, nil
		}
		return parts, nil
	case Negate:
		inner, err := qdrantConds(c.Part)
		if err != nil {
			return nil, err
		}
		if len(inner) != 1 {
			return nil, &ErrNotTranslatable{Reason: "negating a compound condition on Qdrant"}
		}
		inner[0].bucket = map[string]string{"must": "must_not", "must_not": "must", "should": "must_not"}[inner[0].bucket]
		return inner, nil
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
