package orm

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"time"
)

// ErrStop can be returned from a Chunk callback to stop without an error.
var ErrStop = errors.New("orm: stop iteration")

// Get runs the query and returns all matching models with eager loads applied.
func (q Query[M]) Get(ctx context.Context) ([]M, error) {
	q = q.prepared()
	if models, err := q.findDocs(ctx); err != nil || models != nil {
		return models, err
	}
	rows, err := q.query(ctx, func(b *SQL) { q.renderSelect(b, nil) })
	if err != nil {
		return nil, err
	}
	models, err := q.scan(rows)
	if err != nil {
		return nil, err
	}
	if err := q.finish(ctx, models); err != nil {
		return nil, err
	}
	return models, nil
}

func (q Query[M]) scan(rows *sql.Rows) ([]M, error) {
	defer closeRows(rows)
	dests := q.dests()
	var models []M
	for rows.Next() {
		var m M
		ptrs := make([]any, len(dests))
		for i, d := range dests {
			p := q.table.Ptr(&m, d)
			if p == nil {
				return nil, fmt.Errorf("orm: %s has no field for selected column %q", q.table.Name, d)
			}
			ptrs[i] = nullable{p}
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		q.table.sync(&m)
		models = append(models, m)
	}
	return models, rows.Err()
}

// finish runs eager loads and Retrieved events.
func (q Query[M]) finish(ctx context.Context, models []M) error {
	for _, load := range q.eager {
		if err := load(ctx, q.onConn, models); err != nil {
			return err
		}
	}
	for i := range models {
		if err := q.table.fire(ctx, Retrieved, &models[i]); err != nil {
			return err
		}
	}
	return nil
}

// All returns every row (Model::all()).
func (t *Table[M]) All(ctx context.Context) ([]M, error) { return t.Query().Get(ctx) }

// First returns the first matching model or ErrNotFound (first / firstOrFail).
func (q Query[M]) First(ctx context.Context) (M, error) {
	models, err := q.Limit(1).Get(ctx)
	if err != nil || len(models) == 0 {
		var zero M
		return zero, cmp.Or(err, ErrNotFound)
	}
	return models[0], nil
}

// FirstOr returns the first match, or the result of fn if there is none.
func (q Query[M]) FirstOr(ctx context.Context, fn func() (M, error)) (M, error) {
	m, err := q.First(ctx)
	if errors.Is(err, ErrNotFound) {
		return fn()
	}
	return m, err
}

// Sole returns the only matching model; ErrNotFound or ErrMultipleRecords otherwise.
func (q Query[M]) Sole(ctx context.Context) (M, error) { return sole(q.Limit(2).Get(ctx)) }

// sole is the one item of items: ErrNotFound for none, ErrMultipleRecords
// for more.
func sole[T any](items []T, err error) (T, error) {
	var zero T
	switch {
	case err != nil:
		return zero, err
	case len(items) == 0:
		return zero, ErrNotFound
	case len(items) > 1:
		return zero, ErrMultipleRecords
	}
	return items[0], nil
}

// Find returns the model with the given primary key (find / findOrFail).
func (q Query[M]) Find(ctx context.Context, id any) (M, error) { return q.WhereKey(id).First(ctx) }

// FindOr returns the model with the given primary key, or the result of fn
// when there is none (findOr).
func (q Query[M]) FindOr(ctx context.Context, id any, fn func() (M, error)) (M, error) {
	m, err := q.Find(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return fn()
	}
	return m, err
}

// FindMany returns the models with the given primary keys.
func (q Query[M]) FindMany(ctx context.Context, ids ...any) ([]M, error) {
	return q.WhereKey(ids...).Get(ctx)
}

func (t *Table[M]) Find(ctx context.Context, id any) (M, error) { return t.Query().Find(ctx, id) }

func (t *Table[M]) Where(conds ...Cond[M]) Query[M] { return t.Query().Where(conds...) }

// ---------------------------------------------------------------------------
// Aggregates and single values
// ---------------------------------------------------------------------------

func (q Query[M]) Exists(ctx context.Context) (bool, error) {
	q = q.prepared()
	if n, ok, err := q.countDocs(ctx); ok {
		return err == nil && n > 0, err
	}
	q.extras, q.eager = nil, nil
	rows, err := q.Limit(1).query(ctx, func(b *SQL) { q.Limit(1).renderSelect(b, selectOne) })
	if err != nil {
		return false, err
	}
	defer closeRows(rows)
	return rows.Next(), rows.Err()
}

func (q Query[M]) DoesntExist(ctx context.Context) (bool, error) {
	ok, err := q.Exists(ctx)
	return !ok, err
}

func (q Query[M]) Count(ctx context.Context) (int64, error) {
	q = q.prepared()
	if n, ok, err := q.countDocs(ctx); ok {
		return n, err
	}
	if len(q.groups) > 0 || q.distinct || len(q.unions) > 0 {
		inner := q.forAggregate()
		return scalarOn[int64](ctx, q.connName(), func(b *SQL) {
			b.Write("SELECT COUNT(*) FROM (")
			inner.renderSelect(b, nil)
			b.Write(") AS ")
			b.Ident("aggregate")
		})
	}
	return selectScalar[int64](ctx, q, Count[M]().build)
}

// Sum only accepts numeric expressions: Sum(ctx, Users.Email) does not compile.
func (q Query[M]) Sum[V Number](ctx context.Context, e Expr[M, V]) (V, error) {
	return aggregate[V](ctx, q, AggregateSum, e)
}

func (q Query[M]) Avg[V Number](ctx context.Context, e Expr[M, V]) (float64, error) {
	return aggregate[float64](ctx, q, AggregateAvg, e)
}

func (q Query[M]) Min[V any](ctx context.Context, e Expr[M, V]) (V, error) {
	return aggregate[V](ctx, q, AggregateMin, e)
}

func (q Query[M]) Max[V any](ctx context.Context, e Expr[M, V]) (V, error) {
	return aggregate[V](ctx, q, AggregateMax, e)
}

// Pluck returns one expression per row; V comes from the expression, so
// Pluck(ctx, Users.Email) is a []string.
func (q Query[M]) Pluck[V any](ctx context.Context, e Expr[M, V]) ([]V, error) {
	q = q.prepared()
	if docs, cols, ok, err := q.pluckDocs(ctx, e); err != nil || ok {
		if err != nil {
			return nil, err
		}
		out := make([]V, len(docs))
		for i, d := range docs {
			if err := (nullable{&out[i]}).Scan(d[cols[0]]); err != nil {
				return nil, fmt.Errorf("orm: pluck %s from %s: %w", cols[0], q.table.Name, err)
			}
		}
		return out, nil
	}
	rows, err := q.query(ctx, func(b *SQL) { q.renderSelect(b, e.build) })
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var out []V
	for rows.Next() {
		var v V
		if err := rows.Scan(nullable{&v}); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// PluckMap is pluck('value', 'key').
func (q Query[M]) PluckMap[K comparable, V any](ctx context.Context, key Expr[M, K], val Expr[M, V]) (map[K]V, error) {
	q = q.prepared()
	if docs, cols, ok, err := q.pluckDocs(ctx, val, key); err != nil || ok {
		if err != nil {
			return nil, err
		}
		out := make(map[K]V, len(docs))
		for _, d := range docs {
			var k K
			var v V
			if err := (nullable{&v}).Scan(d[cols[0]]); err != nil {
				return nil, fmt.Errorf("orm: pluck %s from %s: %w", cols[0], q.table.Name, err)
			}
			if err := (nullable{&k}).Scan(d[cols[1]]); err != nil {
				return nil, fmt.Errorf("orm: pluck %s from %s: %w", cols[1], q.table.Name, err)
			}
			out[k] = v
		}
		return out, nil
	}
	rows, err := q.query(ctx, func(b *SQL) {
		q.renderSelect(b, func(b *SQL) { val.build(b); b.Write(", "); key.build(b) })
	})
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	out := map[K]V{}
	for rows.Next() {
		var k K
		var v V
		if err := rows.Scan(nullable{&v}, nullable{&k}); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (q Query[M]) pluckDocs(ctx context.Context, exprs ...AnyExpr) ([]map[string]any, []string, bool, error) {
	ds, ok, err := q.docstore(ctx)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	cols := make([]string, len(exprs))
	for i, e := range exprs {
		f, ok := e.(interface{ docColumn() string })
		if !ok || f.docColumn() == "" {
			return nil, nil, true, fmt.Errorf("orm: pluck on %s: %w", q.table.Name, &ErrNotTranslatable{Reason: "plucking an expression rather than a column"})
		}
		cols[i] = f.docColumn()
	}
	p, err := q.planOf("pluck")
	if err != nil {
		return nil, nil, true, err
	}
	p.Columns = cols
	start := time.Now()
	docs, err := ds.FindDocs(ctx, p)
	emitDoc(q.connName(), "pluck", p, time.Since(start), err)
	return docs, cols, true, err
}

// Value returns one expression from the first row.
func (q Query[M]) Value[V any](ctx context.Context, e Expr[M, V]) (V, error) {
	vs, err := q.Limit(1).Pluck(ctx, e)
	if err != nil || len(vs) == 0 {
		var zero V
		return zero, cmp.Or(err, ErrNotFound)
	}
	return vs[0], nil
}

// KeyBy returns the models indexed by a column.
func (q Query[M]) KeyBy[K comparable](ctx context.Context, col Column[M, K]) (map[K]M, error) {
	models, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[K]M, len(models))
	for i := range models {
		out[col.Get(&models[i])] = models[i]
	}
	return out, nil
}

// Grouped returns the models grouped by a column (collection groupBy; the
// name GroupBy is the SQL clause).
func (q Query[M]) Grouped[K comparable](ctx context.Context, col Column[M, K]) (map[K][]M, error) {
	models, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}
	out := map[K][]M{}
	for i := range models {
		k := col.Get(&models[i])
		out[k] = append(out[k], models[i])
	}
	return out, nil
}

// Map runs the query and transforms each model.
func (q Query[M]) Map[R any](ctx context.Context, fn func(M) R) ([]R, error) {
	models, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]R, len(models))
	for i, m := range models {
		out[i] = fn(m)
	}
	return out, nil
}

func (q Query[M]) forAggregate() Query[M] {
	q.orders, q.limit, q.offset, q.eager, q.extras = nil, 0, 0, nil, nil
	return q
}

func aggregate[V, M any](ctx context.Context, q Query[M], agg Aggregate, e AnyExpr) (V, error) {
	q = q.prepared().forAggregate()
	ds, ok, err := q.docstore(ctx)
	if err != nil {
		var zero V
		return zero, err
	}
	if !ok {
		v, err := optionalOn[V](ctx, q.connName(), func(b *SQL) { q.renderSelect(b, func(b *SQL) { fn(b, agg.String(), e) }) })
		if err != nil {
			var zero V
			return zero, err
		}
		return aggregated(agg, v, q.table.Name)
	}
	method := strings.ToLower(agg.String())
	f, ok := e.(interface{ docColumn() string })
	if !ok || f.docColumn() == "" {
		var zero V
		return zero, fmt.Errorf("orm: %s on %s: %w", method, q.table.Name, &ErrNotTranslatable{Reason: "aggregating an expression rather than a column"})
	}
	p, err := q.planOf(method)
	if err != nil {
		var zero V
		return zero, err
	}
	p.Columns = nil
	return aggregateDocs[V](ctx, ds, q.connName(), p, agg, f.docColumn())
}

func selectScalar[V, M any](ctx context.Context, q Query[M], list frag) (V, error) {
	q = q.forAggregate()
	return scalarOn[V](ctx, q.connName(), func(b *SQL) { q.renderSelect(b, list) })
}

// Explain returns the database's query plan for the query as one map of
// column name to value per row (explain()).
func (q Query[M]) Explain(ctx context.Context) ([]map[string]any, error) {
	if _, ok, err := q.docstore(ctx); err != nil {
		return nil, err
	} else if ok {
		return nil, &ErrNotTranslatable{Reason: "explain"}
	}
	q = q.prepared()
	q.extras, q.eager = nil, nil
	return explainOn(ctx, q.connName(), func(b *SQL) { q.renderSelect(b, nil) })
}

// explainOn returns the engine's plan for the SELECT query renders.
func explainOn(ctx context.Context, conn string, query frag) ([]map[string]any, error) {
	rows, err := readRows(ctx, conn, func(b *SQL) {
		b.Write("EXPLAIN ")
		query(b)
	})
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	return scanMaps(rows)
}

// ---------------------------------------------------------------------------
// Chunking and iteration
// ---------------------------------------------------------------------------

// Chunk processes results in pages of size (ordered queries only).
// Return ErrStop from fn to stop early.
func (q Query[M]) Chunk(ctx context.Context, size int, fn func([]M) error) error {
	return chunkPages(size, func(limit, offset int) ([]M, error) { return q.Limit(limit).Offset(offset).Get(ctx) }, fn)
}

// ChunkByID pages by primary key, safe when fn modifies the rows it gets.
func (q Query[M]) ChunkByID(ctx context.Context, size int, fn func([]M) error) error {
	return eachChunk(q.chunksByID(ctx, size), fn)
}

// chunkPages hands fn the pages of size rows that page fetches by offset,
// until a short page.
func chunkPages[T any](size int, page func(limit, offset int) ([]T, error), fn func([]T) error) error {
	if size < 1 {
		return fmt.Errorf("orm: chunk size must be at least 1, got %d", size)
	}
	for offset := 0; ; offset += size {
		items, err := page(size, offset)
		if err != nil {
			return err
		}
		if len(items) > 0 {
			if err := fn(items); err != nil {
				return ignoreStop(err)
			}
		}
		if len(items) < size {
			return nil
		}
	}
}

func eachChunk[T any](chunks iter.Seq2[[]T, error], fn func([]T) error) error {
	for items, err := range chunks {
		if err != nil {
			return err
		}
		if err := fn(items); err != nil {
			return ignoreStop(err)
		}
	}
	return nil
}

// keysetChunks pages by a key column in ascending order: fetch returns the
// size rows after the given key (nil: from the start), key reads a row's
// key, so rows changed or deleted by the consumer never shift the pages.
func keysetChunks[T any](size int, fetch func(after any) ([]T, error), key func(T) (any, error)) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		if size < 1 {
			yield(nil, fmt.Errorf("orm: chunk size must be at least 1, got %d", size))
			return
		}
		var after any
		for {
			items, err := fetch(after)
			if err != nil {
				yield(nil, err)
				return
			}
			if len(items) == 0 || !yield(items, nil) || len(items) < size {
				return
			}
			if after, err = key(items[len(items)-1]); err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

// flatten streams the items of chunks one at a time.
func flatten[T any](chunks iter.Seq2[[]T, error]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for items, err := range chunks {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			for _, it := range items {
				if !yield(it, nil) {
					return
				}
			}
		}
	}
}

// Lazy streams models chunk by chunk (lazyById), with eager loads per chunk:
//
//	for u, err := range Users.Query().With(UserPosts).Lazy(ctx, 500) { ... }
func (q Query[M]) Lazy(ctx context.Context, size int) iter.Seq2[M, error] {
	return flatten(q.chunksByID(ctx, size))
}

func (q Query[M]) chunksByID(ctx context.Context, size int) iter.Seq2[[]M, error] {
	page := q.Reorder(q.table.keyOrder()).Limit(size)
	return keysetChunks(size, func(after any) ([]M, error) {
		if after == nil {
			return page.Get(ctx)
		}
		return page.Where(q.table.column(q.table.PrimaryKey).Gt(after)).Get(ctx)
	}, func(m M) (any, error) { return q.table.key(&m), nil })
}

// ChunkWhile processes results ordered by primary key, splitting into a
// new chunk whenever compare is false for two consecutive rows
// (chunkWhile). Return ErrStop from fn to stop early.
func (q Query[M]) ChunkWhile(ctx context.Context, compare func(prev, cur M) bool, fn func([]M) error) error {
	var batch []M
	flush := func() error {
		if len(batch) > 0 {
			if err := fn(batch); err != nil {
				return ignoreStop(err)
			}
			batch = nil
		}
		return nil
	}
	for models, err := range q.chunksByID(ctx, 500) {
		if err != nil {
			return err
		}
		for _, m := range models {
			if len(batch) > 0 && !compare(batch[len(batch)-1], m) {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, m)
		}
	}
	return flush()
}

// Cursor streams rows one at a time over a single query, without eager
// loading (cursor()).
func (q Query[M]) Cursor(ctx context.Context) iter.Seq2[M, error] {
	return func(yield func(M, error) bool) {
		var zero M
		q := q.prepared()
		rows, err := q.query(ctx, func(b *SQL) { q.renderSelect(b, nil) })
		if err != nil {
			yield(zero, err)
			return
		}
		defer closeRows(rows)
		dests := q.dests()
		for rows.Next() {
			var m M
			ptrs := make([]any, len(dests))
			for i, d := range dests {
				ptrs[i] = nullable{q.table.Ptr(&m, d)}
			}
			if err := rows.Scan(ptrs...); err != nil {
				yield(zero, err)
				return
			}
			q.table.sync(&m)
			if err := q.table.fire(ctx, Retrieved, &m); err != nil {
				yield(zero, err)
				return
			}
			if !yield(m, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(zero, err)
		}
	}
}

func ignoreStop(err error) error {
	if errors.Is(err, ErrStop) {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

// Page is a length-aware page (paginate()), shaped like Laravel's JSON.
// From and To are the 1-based positions of the first and last item in the
// whole result, both 0 on an empty page.
type Page[M any] struct {
	Items       []M   `json:"data"`
	Total       int64 `json:"total"`
	PerPage     int   `json:"per_page"`
	CurrentPage int   `json:"current_page"`
	LastPage    int   `json:"last_page"`
	From        int64 `json:"from"`
	To          int64 `json:"to"`
	HasMore     bool  `json:"has_more"`
}

// SimplePage is a page without a total count (simplePaginate()).
type SimplePage[M any] struct {
	Items       []M  `json:"data"`
	PerPage     int  `json:"per_page"`
	CurrentPage int  `json:"current_page"`
	HasMore     bool `json:"has_more"`
}

// CursorPage is a keyset page (cursorPaginate()).
type CursorPage[M any] struct {
	Items      []M    `json:"data"`
	PerPage    int    `json:"per_page"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// Paginate returns page (from 1) of perPage models with the total count;
// it works on every engine, document stores included.
func (q Query[M]) Paginate(ctx context.Context, perPage, page int) (Page[M], error) {
	return paginate(perPage, page, func() (int64, error) { return q.Count(ctx) },
		func(limit, offset int) ([]M, error) { return q.Limit(limit).Offset(offset).Get(ctx) })
}

// SimplePaginate is Paginate without the count query: it only reports
// whether another page follows.
func (q Query[M]) SimplePaginate(ctx context.Context, perPage, page int) (SimplePage[M], error) {
	return simplePaginate(perPage, page, func(limit, offset int) ([]M, error) { return q.Limit(limit).Offset(offset).Get(ctx) })
}

func checkPage(perPage, page int) error {
	if perPage < 1 || page < 1 {
		return fmt.Errorf("orm: paginate needs perPage and page of at least 1, got %d and %d", perPage, page)
	}
	return nil
}

func paginate[T any](perPage, page int, count func() (int64, error), get func(limit, offset int) ([]T, error)) (Page[T], error) {
	if err := checkPage(perPage, page); err != nil {
		return Page[T]{}, err
	}
	total, err := count()
	if err != nil {
		return Page[T]{}, err
	}
	offset := (page - 1) * perPage
	items, err := get(perPage, offset)
	if err != nil {
		return Page[T]{}, err
	}
	last := max(1, int((total+int64(perPage)-1)/int64(perPage)))
	p := Page[T]{Items: items, Total: total, PerPage: perPage, CurrentPage: page, LastPage: last, HasMore: page < last}
	if len(items) > 0 {
		p.From = int64(offset) + 1
		p.To = int64(offset + len(items))
	}
	return p, nil
}

func simplePaginate[T any](perPage, page int, get func(limit, offset int) ([]T, error)) (SimplePage[T], error) {
	if err := checkPage(perPage, page); err != nil {
		return SimplePage[T]{}, err
	}
	items, err := get(perPage+1, (page-1)*perPage)
	if err != nil {
		return SimplePage[T]{}, err
	}
	more := len(items) > perPage
	if more {
		items = items[:perPage]
	}
	return SimplePage[T]{Items: items, PerPage: perPage, CurrentPage: page, HasMore: more}, nil
}

// CursorPaginate pages by the values of the ORDER BY columns (keyset
// pagination). Orders must be plain columns; with none, the primary key is used.
func (q Query[M]) CursorPaginate(ctx context.Context, perPage int, cursor string) (CursorPage[M], error) {
	orders := q.orders
	if len(orders) == 0 {
		orders = []Order[M]{q.table.keyOrder()}
		q = q.Reorder(orders...)
	}
	terms := make([]keysetTerm, len(orders))
	for i, o := range orders {
		if o.col == nil {
			return CursorPage[M]{}, errors.New("orm: CursorPaginate needs column orders")
		}
		terms[i] = keysetTerm{e: o.col, col: o.col.Name(), desc: o.desc}
	}
	return cursorPaginate(perPage, cursor, terms,
		func(i int, raw json.RawMessage) (any, error) { return orders[i].col.decode(raw) },
		func(after *Cond[M], limit int) ([]M, error) {
			page := q.Limit(limit)
			if after != nil {
				page = page.Where(*after)
			}
			return page.Get(ctx)
		},
		func(m M, i int) any { return orders[i].col.anyValue(&m) })
}

// keysetTerm is one ORDER BY column of a keyset (cursor) page.
type keysetTerm struct {
	e    AnyExpr
	col  string
	desc bool
}

// cursorPaginate fetches perPage items after cursor in the order of terms:
// decode reads term i's value back from the cursor, fetch runs the page
// query (after is nil on the first page), value reads term i from an item.
func cursorPaginate[T, M any](perPage int, cursor string, terms []keysetTerm,
	decode func(i int, raw json.RawMessage) (any, error),
	fetch func(after *Cond[M], limit int) ([]T, error),
	value func(item T, i int) any,
) (CursorPage[T], error) {
	if perPage < 1 {
		return CursorPage[T]{}, fmt.Errorf("orm: CursorPaginate needs perPage of at least 1, got %d", perPage)
	}
	var after *Cond[M]
	if cursor != "" {
		vals, err := decodeCursor(cursor, len(terms), decode)
		if err != nil {
			return CursorPage[T]{}, err
		}
		c := keysetAfter[M](terms, vals)
		after = &c
	}
	items, err := fetch(after, perPage+1)
	if err != nil {
		return CursorPage[T]{}, err
	}
	page := CursorPage[T]{Items: items, PerPage: perPage}
	if len(items) <= perPage {
		return page, nil
	}
	page.Items = items[:perPage]
	vals := make([]any, len(terms))
	for i := range terms {
		vals[i] = value(items[perPage-1], i)
	}
	raw, err := json.Marshal(vals)
	if err != nil {
		return CursorPage[T]{}, fmt.Errorf("orm: encoding the next cursor: %w", err)
	}
	page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	return page, nil
}

func decodeCursor(cursor string, n int, decode func(i int, raw json.RawMessage) (any, error)) ([]any, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("orm: bad cursor: %w", err)
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, fmt.Errorf("orm: bad cursor: %w", err)
	}
	if len(parts) != n {
		return nil, fmt.Errorf("orm: bad cursor: %d values for %d order columns", len(parts), n)
	}
	vals := make([]any, n)
	for i, p := range parts {
		if vals[i], err = decode(i, p); err != nil {
			return nil, fmt.Errorf("orm: bad cursor: %w", err)
		}
	}
	return vals, nil
}

// keysetAfter is the condition for rows after vals in the order of terms —
// (a > x) OR (a = x AND b > y) ..., with < for descending terms — in its
// SQL and document-store forms.
func keysetAfter[M any](terms []keysetTerm, vals []any) Cond[M] {
	ors := make([]Cond[M], len(terms))
	for i := range terms {
		ands := make([]Cond[M], i+1)
		for j := 0; j <= i; j++ {
			op := "="
			if j == i {
				op = map[bool]string{false: ">", true: "<"}[terms[j].desc]
			}
			t, v := terms[j], vals[j]
			ands[j] = Cond[M]{
				f:  func(b *SQL) { t.e.build(b); b.Write(" ", op, " "); b.Arg(v) },
				ir: Field{Column: t.col, Op: op, Value: v},
			}
		}
		ors[i] = And(ands...)
	}
	return Or(ors...)
}

func decodeInto(raw json.RawMessage, ptr any) (any, error) {
	v := reflect.New(reflect.TypeOf(ptr).Elem())
	if err := json.Unmarshal(raw, v.Interface()); err != nil {
		return nil, err
	}
	return v.Elem().Interface(), nil
}
