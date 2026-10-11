package orm

import (
	"context"
	"fmt"
	"slices"
)

// Loader is a relation that can be eager loaded onto a slice of M.
// Its methods are unexported and non-generic: generic methods cannot satisfy
// interfaces, so the generic surface (With[R], WhereHas[R], ...) lives on
// Query and dispatches here.
type Loader[M, R any, RK comparable] interface {
	eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) error
}

// Relation is a Loader that can also be expressed as a subquery correlated
// to the parent row — which powers whereHas, withCount, withSum, ...
type Relation[M, R any, RK comparable] interface {
	Loader[M, R, RK]
	correlate(scope func(Query[R, RK]) Query[R, RK]) Query[R, RK]
}

// selfAlias aliases the inner table when a relation points at its own table.
const selfAlias = "laravel_reserved_0"

// throughAlias aliases a through relation's intermediate table when it is
// the parent's own.
const throughAlias = "laravel_reserved_1"

// ---------------------------------------------------------------------------
// Query methods using relations
// ---------------------------------------------------------------------------

// With eager-loads a relation. Scopes constrain the related query, and
// nested eager loads are just With calls inside a scope.
func (q Query[M, MK]) With[R any, RK comparable](rel Loader[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	scope := chain(scopes)
	q.eager = append(slices.Clip(q.eager), func(ctx context.Context, conn string, models []M) error {
		return rel.eagerLoad(ctx, conn, models, scope)
	})
	return q
}

// Load eager-loads a relation onto already retrieved models ($models->load()).
func Load[M, R any, RK comparable](ctx context.Context, models []M, rel Loader[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) error {
	return rel.eagerLoad(ctx, "", models, chain(scopes))
}

// WithCount selects the number of related rows into a virtual column.
func (q Query[M, MK]) WithCount[R any, RK comparable](rel Relation[M, R, RK], into Column[M, MK, int64], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.AddSelectAs(into, relAggregate[int64](rel, scopes, func(b *SQL) { b.Write("COUNT(*)") }))
}

// WithExists selects whether any related row exists into a virtual column.
func (q Query[M, MK]) WithExists[R any, RK comparable](rel Relation[M, R, RK], into Column[M, MK, bool], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	sub := rel.correlate(chain(scopes)).forAggregate()
	return q.AddSelectAs(into, Scalar[M, bool]{f: func(b *SQL) {
		b.Write("EXISTS (")
		sub.renderSelect(b, selectOne)
		b.Write(")")
	}})
}

func (q Query[M, MK]) WithSum[R any, RK comparable, V Number](rel Relation[M, R, RK], col Column[R, RK, V], into Column[M, MK, V], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.AddSelectAs(into, relAggregate[V](rel, scopes, Sum(col).build))
}

func (q Query[M, MK]) WithAvg[R any, RK comparable, V Number](rel Relation[M, R, RK], col Column[R, RK, V], into Column[M, MK, float64], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.AddSelectAs(into, relAggregate[float64](rel, scopes, Avg(col).build))
}

func (q Query[M, MK]) WithMin[R any, RK comparable, V any](rel Relation[M, R, RK], col Column[R, RK, V], into Column[M, MK, V], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.AddSelectAs(into, relAggregate[V](rel, scopes, Min(col).build))
}

func (q Query[M, MK]) WithMax[R any, RK comparable, V any](rel Relation[M, R, RK], col Column[R, RK, V], into Column[M, MK, V], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.AddSelectAs(into, relAggregate[V](rel, scopes, Max(col).build))
}

// relAggregate is COALESCE((SELECT agg FROM related WHERE correlated), zero).
func relAggregate[V, M, R any, RK comparable](rel Relation[M, R, RK], scopes []func(Query[R, RK]) Query[R, RK], agg frag) Subquery[V] {
	sub := rel.correlate(chain(scopes)).forAggregate()
	var zero V
	return Subquery[V]{func(b *SQL) {
		b.Write("SELECT COALESCE((")
		sub.renderSelect(b, agg)
		b.Write("), ")
		b.Arg(zero)
		b.Write(")")
	}}
}

// Has is the condition "has at least one related row matching scopes"; use
// it to compose (OrWhere(orm.Has(...))). WhereHas is the common shorthand.
func Has[M, R any, RK comparable](rel Relation[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Cond[M] {
	return Exists[M](rel.correlate(chain(scopes)))
}

func DoesntHave[M, R any, RK comparable](rel Relation[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Cond[M] {
	return Not(Has(rel, scopes...))
}

func (q Query[M, MK]) WhereHas[R any, RK comparable](rel Relation[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.Where(Has(rel, scopes...))
}

func (q Query[M, MK]) OrWhereHas[R any, RK comparable](rel Relation[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.OrWhere(Has(rel, scopes...))
}

func (q Query[M, MK]) WhereDoesntHave[R any, RK comparable](rel Relation[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.Where(DoesntHave(rel, scopes...))
}

func (q Query[M, MK]) OrWhereDoesntHave[R any, RK comparable](rel Relation[M, R, RK], scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	return q.OrWhere(DoesntHave(rel, scopes...))
}

// WhereRelation is whereHas with plain conditions on the related model.
func (q Query[M, MK]) WhereRelation[R any, RK comparable](rel Relation[M, R, RK], conds ...Cond[R]) Query[M, MK] {
	return q.WhereHas(rel, func(r Query[R, RK]) Query[R, RK] { return r.Where(conds...) })
}

// HasCount is has('posts', '>=', 3): compares the related row count.
func (q Query[M, MK]) HasCount[R any, RK comparable](rel Relation[M, R, RK], op Op, n int64, scopes ...func(Query[R, RK]) Query[R, RK]) Query[M, MK] {
	sub := relAggregate[int64](rel, scopes, func(b *SQL) { b.Write("COUNT(*)") })
	return q.Where(Cond[M]{f: func(b *SQL) { compare(b, sub, op, n) }})
}

// ---------------------------------------------------------------------------
// HasMany / HasOne / MorphMany / MorphOne
// ---------------------------------------------------------------------------

type hasBase[M any, MK comparable, R any, RK comparable, J comparable] struct {
	bound
	related    *Table[R, RK]
	local      Column[M, MK, J]
	foreign    Column[R, RK, J]
	morphType  *Column[R, RK, string] // set for MorphOne/MorphMany
	scopes     []func(Query[R, RK]) Query[R, RK]
	attributes []Assignment[R]
	inverse    func(child *R, parent *M)
}

func (h hasBase[M, MK, R, RK, J]) constrain(q Query[R, RK]) Query[R, RK] {
	if h.morphType != nil {
		q = q.Where(h.morphType.Eq(h.local.table.morphAlias()))
	}
	return q.Scope(h.scopes...)
}

func (h hasBase[M, MK, R, RK, J]) load(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) (map[J][]R, error) {
	keys := uniqueKeys(parents, h.local, false)
	if len(keys) == 0 {
		return nil, nil
	}
	q := scope(h.constrain(relQuery(h.related, conn).Where(h.foreign.In(keys...))))
	if q.limit > 0 || q.offset > 0 {
		q.perParent = h.foreign
	}
	children, err := q.WithContext(ctx).Get()
	if err != nil {
		return nil, err
	}
	byKey := map[J][]R{}
	for i := range children {
		k := h.foreign.Get(&children[i])
		byKey[k] = append(byKey[k], children[i])
	}
	return byKey, nil
}

func (h hasBase[M, MK, R, RK, J]) correlate(scope func(Query[R, RK]) Query[R, RK]) Query[R, RK] {
	q := h.related.Query()
	if h.related.Name == h.local.table.Name {
		q.alias = selfAlias
	}
	return scope(h.constrain(q.Where(h.foreign.EqCol(h.local.outer()))))
}

// Of is the relationship query for one parent: UserPosts.Of(&u) is $user->posts().
func (h hasBase[M, MK, R, RK, J]) Of(parent *M) Query[R, RK] {
	return h.constrain(h.related.Query().Where(h.foreign.Eq(h.local.Get(parent))))
}

// Create sets the foreign key (and morph type) on child and inserts it.
func (h hasBase[M, MK, R, RK, J]) Create(parent *M, child *R) error {
	ctx := h.context()
	h.link(parent, child)
	return h.related.WithContext(ctx).Create(child)
}

// Save sets the foreign key on child and saves it.
func (h hasBase[M, MK, R, RK, J]) Save(parent *M, child *R) error {
	ctx := h.context()
	h.link(parent, child)
	return h.related.WithContext(ctx).Save(child)
}

func (h hasBase[M, MK, R, RK, J]) link(parent *M, child *R) {
	fill(child, h.attributes)
	*h.foreign.Ptr(child) = h.local.Get(parent)
	if h.morphType != nil {
		*h.morphType.Ptr(child) = h.local.table.morphAlias()
	}
}

// HasManyRel is a one-to-many relation (hasMany / morphMany).
type HasManyRel[M any, MK comparable, R any, RK comparable, J comparable] struct {
	hasBase[M, MK, R, RK, J]
	set func(*M, []R)
}

// HasMany: parent M has many R where R.foreign = M.local.
//
//	var UserPosts = orm.HasMany(Users.ID, Posts.UserID,
//		func(u *User, ps []Post) { u.Posts = ps })
//
// The related table is foreign's, so it cannot disagree with the columns.
func HasMany[M any, MK comparable, R any, RK comparable, J comparable](local Column[M, MK, J], foreign Column[R, RK, J], set func(*M, []R)) *HasManyRel[M, MK, R, RK, J] {
	return &HasManyRel[M, MK, R, RK, J]{hasBase[M, MK, R, RK, J]{related: foreign.table, local: local, foreign: foreign}, set}
}

// MorphMany: parent M has many polymorphic R (R.morphID = M.local AND R.morphType = M's alias).
func MorphMany[M any, MK comparable, R any, RK comparable, J comparable](local Column[M, MK, J], morphID Column[R, RK, J], morphType Column[R, RK, string], set func(*M, []R)) *HasManyRel[M, MK, R, RK, J] {
	r := HasMany(local, morphID, set)
	r.morphType = &morphType
	return r
}

func (r *HasManyRel[M, MK, R, RK, J]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) error {
	byKey, err := r.load(ctx, conn, parents, scope)
	if err != nil {
		return err
	}
	for i := range parents {
		children := byKey[r.local.Get(&parents[i])]
		if children == nil {
			children = []R{}
		}
		if r.inverse != nil {
			children = slices.Clone(children)
			for j := range children {
				r.inverse(&children[j], &parents[i])
			}
		}
		r.set(&parents[i], children)
	}
	return nil
}

// WithContext returns the relation running its writes (Create, Save,
// SaveMany) with ctx: inside the transaction ctx carries.
func (r *HasManyRel[M, MK, R, RK, J]) WithContext(ctx context.Context) *HasManyRel[M, MK, R, RK, J] {
	c := *r
	c.bound = boundTo(ctx)
	return &c
}

// Scoped returns the relation with extra constraints (a "scoped relationship").
func (r *HasManyRel[M, MK, R, RK, J]) Scoped(scopes ...func(Query[R, RK]) Query[R, RK]) *HasManyRel[M, MK, R, RK, J] {
	c := *r
	c.scopes = append(slices.Clip(c.scopes), scopes...)
	return &c
}

// WithAttributes sets attributes on models created through the relation.
func (r *HasManyRel[M, MK, R, RK, J]) WithAttributes(attrs ...Assignment[R]) *HasManyRel[M, MK, R, RK, J] {
	c := *r
	c.attributes = append(slices.Clip(c.attributes), attrs...)
	return &c
}

// Chaperone sets the inverse (child -> parent) when eager loading.
func (r *HasManyRel[M, MK, R, RK, J]) Chaperone(set func(child *R, parent *M)) *HasManyRel[M, MK, R, RK, J] {
	c := *r
	c.inverse = set
	return &c
}

// SaveMany saves each child through the relation.
func (r *HasManyRel[M, MK, R, RK, J]) SaveMany(parent *M, children []R) error {
	ctx := r.context()
	for i := range children {
		if err := r.WithContext(ctx).Save(parent, &children[i]); err != nil {
			return err
		}
	}
	return nil
}

// HasOneRel is a one-to-one relation (hasOne / morphOne / ofMany).
type HasOneRel[M any, MK comparable, R any, RK comparable, J comparable] struct {
	hasBase[M, MK, R, RK, J]
	set    func(*M, *R)
	better func(a, b *R) bool // ofMany: whether a beats b
}

func HasOne[M any, MK comparable, R any, RK comparable, J comparable](local Column[M, MK, J], foreign Column[R, RK, J], set func(*M, *R)) *HasOneRel[M, MK, R, RK, J] {
	return &HasOneRel[M, MK, R, RK, J]{related: foreign.table, local: local, foreign: foreign, set: set}
}

func MorphOne[M any, MK comparable, R any, RK comparable, J comparable](local Column[M, MK, J], morphID Column[R, RK, J], morphType Column[R, RK, string], set func(*M, *R)) *HasOneRel[M, MK, R, RK, J] {
	r := HasOne(local, morphID, set)
	r.morphType = &morphType
	return r
}

// OfMany picks one of many related rows by the max (or min) of col —
// latestOfMany / oldestOfMany / ofMany('price', 'max').
// WithContext returns the relation running its writes (Create, Save)
// with ctx: inside the transaction ctx carries.
func (r *HasOneRel[M, MK, R, RK, J]) WithContext(ctx context.Context) *HasOneRel[M, MK, R, RK, J] {
	c := *r
	c.bound = boundTo(ctx)
	return &c
}

func (r *HasOneRel[M, MK, R, RK, J]) OfMany[V interface {
	~int | ~int64 | ~float64 | ~string
}](col Column[R, RK, V], useMax bool) *HasOneRel[M, MK, R, RK, J] {
	c := *r
	c.better = func(a, b *R) bool {
		if useMax {
			return col.Get(a) > col.Get(b)
		}
		return col.Get(a) < col.Get(b)
	}
	return &c
}

func (r *HasOneRel[M, MK, R, RK, J]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) error {
	byKey, err := r.load(ctx, conn, parents, scope)
	if err != nil {
		return err
	}
	for i := range parents {
		children := byKey[r.local.Get(&parents[i])]
		if len(children) == 0 {
			r.set(&parents[i], nil)
			continue
		}
		best := &children[0]
		for j := range children[1:] {
			if r.better != nil && r.better(&children[j+1], best) {
				best = &children[j+1]
			}
		}
		pick := *best
		if r.inverse != nil {
			r.inverse(&pick, &parents[i])
		}
		r.set(&parents[i], &pick)
	}
	return nil
}

// ---------------------------------------------------------------------------
// BelongsTo
// ---------------------------------------------------------------------------

// BelongsToRel is the inverse of HasOne/HasMany.
type BelongsToRel[M any, MK comparable, R any, RK comparable, J comparable] struct {
	related *Table[R, RK]
	foreign Column[M, MK, J]
	owner   Column[R, RK, J]
	set     func(*M, *R)
	def     func(*M) *R
	scopes  []func(Query[R, RK]) Query[R, RK]
}

// BelongsTo: child M points at one R where M.foreign = R.owner.
func BelongsTo[M any, MK comparable, R any, RK comparable, J comparable](foreign Column[M, MK, J], owner Column[R, RK, J], set func(*M, *R)) *BelongsToRel[M, MK, R, RK, J] {
	return &BelongsToRel[M, MK, R, RK, J]{related: owner.table, foreign: foreign, owner: owner, set: set}
}

// WithDefault supplies a model when the owner is missing (withDefault()).
func (r *BelongsToRel[M, MK, R, RK, J]) WithDefault(fn func(*M) *R) *BelongsToRel[M, MK, R, RK, J] {
	c := *r
	c.def = fn
	return &c
}

func (r *BelongsToRel[M, MK, R, RK, J]) eagerLoad(ctx context.Context, conn string, children []M, scope func(Query[R, RK]) Query[R, RK]) error {
	byKey := map[J]*R{}
	if keys := uniqueKeys(children, r.foreign, true); len(keys) > 0 {
		owners, err := scope(relQuery(r.related, conn).Where(r.owner.In(keys...)).Scope(r.scopes...)).WithContext(ctx).Get()
		if err != nil {
			return err
		}
		for i := range owners {
			byKey[r.owner.Get(&owners[i])] = &owners[i]
		}
	}
	for i := range children {
		o := byKey[r.foreign.Get(&children[i])]
		if o == nil && r.def != nil {
			o = r.def(&children[i])
		}
		r.set(&children[i], o)
	}
	return nil
}

func (r *BelongsToRel[M, MK, R, RK, J]) correlate(scope func(Query[R, RK]) Query[R, RK]) Query[R, RK] {
	q := r.related.Query()
	if r.related.Name == r.foreign.table.Name {
		q.alias = selfAlias
	}
	return scope(q.Where(r.owner.EqCol(r.foreign.outer())).Scope(r.scopes...))
}

// Of is the relationship query for one child ($post->author()).
func (r *BelongsToRel[M, MK, R, RK, J]) Of(child *M) Query[R, RK] {
	return r.related.Query().Where(r.owner.Eq(r.foreign.Get(child))).Scope(r.scopes...)
}

// Associate points child at owner.
func (r *BelongsToRel[M, MK, R, RK, J]) Associate(child *M, owner *R) {
	*r.foreign.Ptr(child) = r.owner.Get(owner)
	r.set(child, owner)
}

// Dissociate clears child's foreign key.
func (r *BelongsToRel[M, MK, R, RK, J]) Dissociate(child *M) {
	var zero J
	*r.foreign.Ptr(child) = zero
	r.set(child, nil)
}

// Is matches children of owner (whereBelongsTo).
func (r *BelongsToRel[M, MK, R, RK, J]) Is(owner *R) Cond[M] { return r.foreign.Eq(r.owner.Get(owner)) }

// Touches makes saving or deleting a child update the owner's updated-at
// ($touches), except under WithoutTimestamps.
func (r *BelongsToRel[M, MK, R, RK, J]) Touches() {
	if r.related.UpdatedAt == "" {
		return
	}
	touch := func(ctx context.Context, m *M) error {
		if !timestamps(ctx) {
			return nil
		}
		_, err := r.related.Query().Where(r.owner.Eq(r.foreign.Get(m))).WithContext(ctx).Update()
		return err
	}
	r.foreign.table.On(Saved, touch)
	r.foreign.table.On(Deleted, touch)
}

// ---------------------------------------------------------------------------
// BelongsToMany / MorphToMany / MorphedByMany
// ---------------------------------------------------------------------------

// BelongsToManyRel is a many-to-many relation through pivot model P.
type BelongsToManyRel[M any, MK comparable, R any, RK comparable, P any, PK comparable, J, RJ comparable] struct {
	bound
	related      *Table[R, RK]
	pivot        *Table[P, PK]
	parentKey    Column[M, MK, J]
	relatedKey   Column[R, RK, RJ]
	pivotParent  Column[P, PK, J]
	pivotRelated Column[P, PK, RJ]
	pivotType    *Column[P, PK, string] // morph pivots
	typeValue    string
	set          func(*M, []R)
	setPivot     func(*R, P)
	pivotWhere   []Cond[P]
	scopes       []func(Query[R, RK]) Query[R, RK]
}

// BelongsToMany: M and R are linked through pivot rows P where
// P.pivotParent = M.parentKey and P.pivotRelated = R.relatedKey.
// The pivot is a regular (generated) model, so pivot columns are typed;
// the related and pivot tables are their columns' tables.
func BelongsToMany[M any, MK comparable, R any, RK comparable, P any, PK comparable, J, RJ comparable](
	parentKey Column[M, MK, J], pivotParent Column[P, PK, J],
	relatedKey Column[R, RK, RJ], pivotRelated Column[P, PK, RJ],
	set func(*M, []R),
) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	return &BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]{
		related: relatedKey.table, pivot: pivotParent.table, parentKey: parentKey, relatedKey: relatedKey,
		pivotParent: pivotParent, pivotRelated: pivotRelated, set: set,
	}
}

// MorphToMany: polymorphic many-to-many from the owning side (Post -> Tags
// via taggables(taggable_id, taggable_type, tag_id)).
func MorphToMany[M any, MK comparable, R any, RK comparable, P any, PK comparable, J, RJ comparable](
	parentKey Column[M, MK, J], pivotMorphID Column[P, PK, J], pivotMorphType Column[P, PK, string],
	relatedKey Column[R, RK, RJ], pivotRelated Column[P, PK, RJ],
	set func(*M, []R),
) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	r := BelongsToMany(parentKey, pivotMorphID, relatedKey, pivotRelated, set)
	r.pivotType, r.typeValue = &pivotMorphType, parentKey.table.morphAlias()
	return r
}

// MorphedByMany: the inverse (Tag -> Posts via taggables).
func MorphedByMany[M any, MK comparable, R any, RK comparable, P any, PK comparable, J, RJ comparable](
	parentKey Column[M, MK, J], pivotParent Column[P, PK, J],
	relatedKey Column[R, RK, RJ], pivotMorphID Column[P, PK, RJ], pivotMorphType Column[P, PK, string],
	set func(*M, []R),
) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	r := BelongsToMany(parentKey, pivotParent, relatedKey, pivotMorphID, set)
	r.pivotType, r.typeValue = &pivotMorphType, relatedKey.table.morphAlias()
	return r
}

// WithPivot hands each loaded related model its pivot row (withPivot / as).
// WithContext returns the relation running its pivot writes (Attach,
// Detach, Sync, Toggle, ...) with ctx: inside the transaction ctx carries.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) WithContext(ctx context.Context) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	c := *r
	c.bound = boundTo(ctx)
	return &c
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) WithPivot(set func(*R, P)) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	c := *r
	c.setPivot = set
	return &c
}

// WherePivot constrains the pivot rows (wherePivot, wherePivotIn, ...).
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) WherePivot(conds ...Cond[P]) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	c := *r
	c.pivotWhere = append(slices.Clip(c.pivotWhere), conds...)
	return &c
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) Scoped(scopes ...func(Query[R, RK]) Query[R, RK]) *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ] {
	c := *r
	c.scopes = append(slices.Clip(c.scopes), scopes...)
	return &c
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) pivotConds() []Cond[P] {
	conds := slices.Clone(r.pivotWhere)
	if r.pivotType != nil {
		conds = append(conds, r.pivotType.Eq(r.typeValue))
	}
	return conds
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) pivotQuery(conn string, parent J) Query[P, PK] {
	return relQuery(r.pivot, conn).Where(r.pivotParent.Eq(parent)).Where(r.pivotConds()...)
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) error {
	keys := uniqueKeys(parents, r.parentKey, false)
	if len(keys) == 0 {
		return nil
	}
	pivots, err := relQuery(r.pivot, conn).Where(r.pivotParent.In(keys...)).Where(r.pivotConds()...).WithContext(ctx).Get()
	if err != nil {
		return err
	}
	byRelated := map[RJ][]P{}
	var rks []RJ
	for i := range pivots {
		rk := r.pivotRelated.Get(&pivots[i])
		if _, seen := byRelated[rk]; !seen {
			rks = append(rks, rk)
		}
		byRelated[rk] = append(byRelated[rk], pivots[i])
	}
	out := map[J][]R{}
	if len(rks) > 0 {
		q := scope(relQuery(r.related, conn).Where(r.relatedKey.In(rks...)).Scope(r.scopes...))
		if err := sharedRowsLimit(q, r.related.Name); err != nil {
			return err
		}
		related, err := q.WithContext(ctx).Get()
		if err != nil {
			return err
		}
		for i := range related {
			for _, p := range byRelated[r.relatedKey.Get(&related[i])] {
				m := related[i]
				if r.setPivot != nil {
					r.setPivot(&m, p)
				}
				k := r.pivotParent.Get(&p)
				out[k] = append(out[k], m)
			}
		}
	}
	for i := range parents {
		rs := out[r.parentKey.Get(&parents[i])]
		if rs == nil {
			rs = []R{}
		}
		r.set(&parents[i], rs)
	}
	return nil
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) correlate(scope func(Query[R, RK]) Query[R, RK]) Query[R, RK] {
	q := r.related.Query()
	if r.related.Name == r.parentKey.table.Name {
		q.alias = selfAlias
	}
	q = q.Join(r.pivot, r.pivotRelated.EqCol(r.relatedKey)).
		WhereOf(r.pivotParent.EqCol(r.parentKey.outer())).
		WhereOf(r.pivotConds()...)
	return scope(q.Scope(r.scopes...))
}

// Of is the relationship query for one parent ($user->roles()).
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) Of(parent *M) Query[R, RK] {
	return r.related.Query().
		Join(r.pivot, r.pivotRelated.EqCol(r.relatedKey)).
		WhereOf(r.pivotParent.Eq(r.parentKey.Get(parent))).
		WhereOf(r.pivotConds()...).
		Scope(r.scopes...)
}

// AttachedTo matches parents attached to related (whereAttachedTo).
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) AttachedTo(related *R) Cond[M] {
	rk := r.relatedKey.Get(related)
	return Has[M, R](r, func(q Query[R, RK]) Query[R, RK] { return q.Where(r.relatedKey.Eq(rk)) })
}

// Attach inserts pivot rows for parent; parent key and morph type are filled
// in, pivot timestamps maintained. Extra pivot columns come from the P values.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) Attach(parent *M, pivots ...P) error {
	ctx := r.context()
	ts := r.pivot.now()
	for i := range pivots {
		*r.pivotParent.Ptr(&pivots[i]) = r.parentKey.Get(parent)
		if r.pivotType != nil {
			*r.pivotType.Ptr(&pivots[i]) = r.typeValue
		}
		for _, c := range []string{r.pivot.CreatedAt, r.pivot.UpdatedAt} {
			if c != "" {
				r.pivot.setValue(&pivots[i], c, ts)
			}
		}
	}
	return r.pivot.Query().WithContext(ctx).Insert(pivots...)
}

// AttachIDs attaches related keys without extra pivot data.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) AttachIDs(parent *M, ids ...RJ) error {
	ctx := r.context()
	pivots := make([]P, len(ids))
	for i, id := range ids {
		*r.pivotRelated.Ptr(&pivots[i]) = id
	}
	return r.WithContext(ctx).Attach(parent, pivots...)
}

// Detach removes pivot rows for ids, or all of parent's when none are given.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) Detach(parent *M, ids ...RJ) (int64, error) {
	ctx := r.context()
	q := r.pivotQuery("", r.parentKey.Get(parent))
	if len(ids) > 0 {
		q = q.Where(r.pivotRelated.In(ids...))
	}
	return q.hardDelete(ctx)
}

// SyncResult reports what Sync and Toggle changed.
type SyncResult[RJ comparable] struct{ Attached, Detached []RJ }

// Sync makes ids exactly the attached set.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) Sync(parent *M, ids ...RJ) (SyncResult[RJ], error) {
	ctx := r.context()
	return r.sync(ctx, parent, ids, true)
}

// SyncWithoutDetaching attaches missing ids and leaves others alone.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) SyncWithoutDetaching(parent *M, ids ...RJ) (SyncResult[RJ], error) {
	ctx := r.context()
	return r.sync(ctx, parent, ids, false)
}

func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) sync(ctx context.Context, parent *M, ids []RJ, detach bool) (SyncResult[RJ], error) {
	var res SyncResult[RJ]
	current, err := r.pivotQuery("", r.parentKey.Get(parent)).WithContext(ctx).Pluck(r.pivotRelated)
	if err != nil {
		return res, err
	}
	for _, id := range ids {
		if !slices.Contains(current, id) && !slices.Contains(res.Attached, id) {
			res.Attached = append(res.Attached, id)
		}
	}
	if detach {
		for _, id := range current {
			if !slices.Contains(ids, id) {
				res.Detached = append(res.Detached, id)
			}
		}
	}
	if len(res.Detached) > 0 {
		if _, err := r.WithContext(ctx).Detach(parent, res.Detached...); err != nil {
			return res, err
		}
	}
	if len(res.Attached) > 0 {
		err = r.WithContext(ctx).AttachIDs(parent, res.Attached...)
	}
	return res, err
}

// Toggle detaches attached ids and attaches the rest.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) Toggle(parent *M, ids ...RJ) (SyncResult[RJ], error) {
	ctx := r.context()
	var res SyncResult[RJ]
	current, err := r.pivotQuery("", r.parentKey.Get(parent)).WithContext(ctx).Pluck(r.pivotRelated)
	if err != nil {
		return res, err
	}
	seen := make(map[RJ]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if slices.Contains(current, id) {
			res.Detached = append(res.Detached, id)
		} else {
			res.Attached = append(res.Attached, id)
		}
	}
	if len(res.Detached) > 0 {
		if _, err := r.WithContext(ctx).Detach(parent, res.Detached...); err != nil {
			return res, err
		}
	}
	if len(res.Attached) > 0 {
		err = r.WithContext(ctx).AttachIDs(parent, res.Attached...)
	}
	return res, err
}

// UpdateExistingPivot updates the pivot row linking parent and id.
func (r *BelongsToManyRel[M, MK, R, RK, P, PK, J, RJ]) UpdateExistingPivot(parent *M, id RJ, sets ...Assignment[P]) (int64, error) {
	ctx := r.context()
	return r.pivotQuery("", r.parentKey.Get(parent)).Where(r.pivotRelated.Eq(id)).WithContext(ctx).Update(sets...)
}

// ---------------------------------------------------------------------------
// HasManyThrough / HasOneThrough
// ---------------------------------------------------------------------------

type throughBase[M any, MK comparable, T any, TK comparable, R any, RK comparable, J, TJ comparable] struct {
	related     *Table[R, RK]
	through     *Table[T, TK]
	localKey    Column[M, MK, J]  // on M
	firstKey    Column[T, TK, J]  // on T, pointing at M
	secondLocal Column[T, TK, TJ] // on T
	secondKey   Column[R, RK, TJ] // on R, pointing at T
}

func (h throughBase[M, MK, T, TK, R, RK, J, TJ]) load(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) (map[J][]R, error) {
	keys := uniqueKeys(parents, h.localKey, false)
	if len(keys) == 0 {
		return nil, nil
	}
	throughs, err := relQuery(h.through, conn).Where(h.firstKey.In(keys...)).WithContext(ctx).Get()
	if err != nil || len(throughs) == 0 {
		return nil, err
	}
	parentsOf := map[TJ][]J{}
	var tks []TJ
	for i := range throughs {
		tk := h.secondLocal.Get(&throughs[i])
		if _, seen := parentsOf[tk]; !seen {
			tks = append(tks, tk)
		}
		parentsOf[tk] = append(parentsOf[tk], h.firstKey.Get(&throughs[i]))
	}
	q := scope(relQuery(h.related, conn).Where(h.secondKey.In(tks...)))
	if err := sharedRowsLimit(q, h.related.Name); err != nil {
		return nil, err
	}
	related, err := q.WithContext(ctx).Get()
	if err != nil {
		return nil, err
	}
	out := map[J][]R{}
	for i := range related {
		for _, k := range parentsOf[h.secondKey.Get(&related[i])] {
			out[k] = append(out[k], related[i])
		}
	}
	return out, nil
}

func (h throughBase[M, MK, T, TK, R, RK, J, TJ]) joined(q Query[R, RK]) Query[R, RK] {
	q = q.Join(h.through, h.secondLocal.EqCol(h.secondKey))
	if t := h.through; t.softDeletes() {
		q = q.WhereOf(Cond[T]{f: func(b *SQL) { b.Col(t.Name, t.DeletedAt); b.Write(" IS NULL") }})
	}
	return q
}

// correlate matches R rows whose secondKey is among the intermediate keys
// of the outer parent: an IN subquery over T rather than a join, so either
// table can be the parent's own and be aliased away from it.
func (h throughBase[M, MK, T, TK, R, RK, J, TJ]) correlate(scope func(Query[R, RK]) Query[R, RK]) Query[R, RK] {
	t := h.through
	keys := t.Query().WithoutGlobalScopes()
	if t.Name == h.localKey.table.Name {
		keys.alias = throughAlias
	}
	if t.softDeletes() {
		keys = keys.Where(t.column(t.DeletedAt).IsNull())
	}
	keys = keys.Where(h.firstKey.EqCol(h.localKey.outer()))
	q := h.related.Query()
	if h.related.Name == h.localKey.table.Name {
		q.alias = selfAlias
	}
	return scope(q.Where(h.secondKey.InSub(keys.Subquery(h.secondLocal))))
}

// Of is the relationship query for one parent.
func (h throughBase[M, MK, T, TK, R, RK, J, TJ]) Of(parent *M) Query[R, RK] {
	return h.joined(h.related.Query()).WhereOf(h.firstKey.Eq(h.localKey.Get(parent)))
}

// HasManyThroughRel reaches R through an intermediate T.
type HasManyThroughRel[M any, MK comparable, T any, TK comparable, R any, RK comparable, J, TJ comparable] struct {
	throughBase[M, MK, T, TK, R, RK, J, TJ]
	set func(*M, []R)
}

// HasManyThrough: M -> T (T.firstKey = M.localKey) -> R (R.secondKey = T.secondLocal).
//
//	var CountryPosts = orm.HasManyThrough(
//		Countries.ID, Users.CountryID, Users.ID, Posts.UserID, set)
func HasManyThrough[M any, MK comparable, T any, TK comparable, R any, RK comparable, J, TJ comparable](
	localKey Column[M, MK, J], firstKey Column[T, TK, J],
	secondLocal Column[T, TK, TJ], secondKey Column[R, RK, TJ],
	set func(*M, []R),
) *HasManyThroughRel[M, MK, T, TK, R, RK, J, TJ] {
	return &HasManyThroughRel[M, MK, T, TK, R, RK, J, TJ]{through(localKey, firstKey, secondLocal, secondKey), set}
}

// through is the shared part of the through relations; the tables are
// their key columns' tables.
func through[M any, MK comparable, T any, TK comparable, R any, RK comparable, J, TJ comparable](localKey Column[M, MK, J], firstKey Column[T, TK, J], secondLocal Column[T, TK, TJ], secondKey Column[R, RK, TJ]) throughBase[M, MK, T, TK, R, RK, J, TJ] {
	return throughBase[M, MK, T, TK, R, RK, J, TJ]{secondKey.table, firstKey.table, localKey, firstKey, secondLocal, secondKey}
}

func (r *HasManyThroughRel[M, MK, T, TK, R, RK, J, TJ]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) error {
	byKey, err := r.load(ctx, conn, parents, scope)
	if err != nil {
		return err
	}
	for i := range parents {
		rs := byKey[r.localKey.Get(&parents[i])]
		if rs == nil {
			rs = []R{}
		}
		r.set(&parents[i], rs)
	}
	return nil
}

// HasOneThroughRel is HasManyThrough returning a single model.
type HasOneThroughRel[M any, MK comparable, T any, TK comparable, R any, RK comparable, J, TJ comparable] struct {
	throughBase[M, MK, T, TK, R, RK, J, TJ]
	set func(*M, *R)
}

func HasOneThrough[M any, MK comparable, T any, TK comparable, R any, RK comparable, J, TJ comparable](
	localKey Column[M, MK, J], firstKey Column[T, TK, J],
	secondLocal Column[T, TK, TJ], secondKey Column[R, RK, TJ],
	set func(*M, *R),
) *HasOneThroughRel[M, MK, T, TK, R, RK, J, TJ] {
	return &HasOneThroughRel[M, MK, T, TK, R, RK, J, TJ]{through(localKey, firstKey, secondLocal, secondKey), set}
}

func (r *HasOneThroughRel[M, MK, T, TK, R, RK, J, TJ]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R, RK]) Query[R, RK]) error {
	byKey, err := r.load(ctx, conn, parents, scope)
	if err != nil {
		return err
	}
	for i := range parents {
		var one *R
		if rs := byKey[r.localKey.Get(&parents[i])]; len(rs) > 0 {
			one = &rs[0]
		}
		r.set(&parents[i], one)
	}
	return nil
}

// ---------------------------------------------------------------------------
// MorphTo
// ---------------------------------------------------------------------------

// MorphTarget is one model a MorphTo relation can point at.
type MorphTarget[J comparable] interface {
	alias() string
	load(ctx context.Context, conn string, keys []J) (map[J]any, error)
	keyOf(owner any) (J, bool)
}

type morphTarget[R any, RK comparable, J comparable] struct{ key Column[R, RK, J] }

// Target declares a MorphTo target by its key column: orm.Target(Posts.ID).
func Target[R any, RK comparable, J comparable](key Column[R, RK, J]) MorphTarget[J] {
	return morphTarget[R, RK, J]{key}
}

func (t morphTarget[R, RK, J]) alias() string { return t.key.table.morphAlias() }

func (t morphTarget[R, RK, J]) load(ctx context.Context, conn string, keys []J) (map[J]any, error) {
	rows, err := relQuery(t.key.table, conn).Where(t.key.In(keys...)).WithContext(ctx).Get()
	if err != nil {
		return nil, err
	}
	out := make(map[J]any, len(rows))
	for i := range rows {
		out[t.key.Get(&rows[i])] = &rows[i]
	}
	return out, nil
}

func (t morphTarget[R, RK, J]) keyOf(owner any) (J, bool) {
	if r, ok := owner.(*R); ok {
		return t.key.Get(r), true
	}
	var zero J
	return zero, false
}

// MorphToRel is the polymorphic inverse: the owner may be any of several
// models, so it is delivered as `any` holding a *Post, *Video, ...
type MorphToRel[M any, MK comparable, J comparable] struct {
	id      Column[M, MK, J]
	typ     Column[M, MK, string]
	set     func(*M, any)
	targets []MorphTarget[J]
}

// MorphTo: comments(commentable_id, commentable_type) -> Post | Video.
func MorphTo[M any, MK comparable, J comparable](id Column[M, MK, J], typ Column[M, MK, string], set func(*M, any), targets ...MorphTarget[J]) *MorphToRel[M, MK, J] {
	return &MorphToRel[M, MK, J]{id, typ, set, targets}
}

func (r *MorphToRel[M, MK, J]) eagerLoad(ctx context.Context, conn string, children []M, _ func(Query[any, NoKey]) Query[any, NoKey]) error {
	byType := map[string][]J{}
	for i := range children {
		t, k := r.typ.Get(&children[i]), r.id.Get(&children[i])
		if !slices.Contains(byType[t], k) {
			byType[t] = append(byType[t], k)
		}
	}
	loaded := map[string]map[J]any{}
	for _, tgt := range r.targets {
		if keys := byType[tgt.alias()]; len(keys) > 0 {
			m, err := tgt.load(ctx, conn, keys)
			if err != nil {
				return err
			}
			loaded[tgt.alias()] = m
		}
	}
	for i := range children {
		r.set(&children[i], loaded[r.typ.Get(&children[i])][r.id.Get(&children[i])])
	}
	return nil
}

// Associate points child at owner, which must be a pointer to a target model.
func (r *MorphToRel[M, MK, J]) Associate(child *M, owner any) bool {
	for _, t := range r.targets {
		if k, ok := t.keyOf(owner); ok {
			*r.id.Ptr(child) = k
			*r.typ.Ptr(child) = t.alias()
			r.set(child, owner)
			return true
		}
	}
	return false
}

// Is matches children whose owner is the given model (whereMorphedTo).
func (r *MorphToRel[M, MK, J]) Is(owner any) Cond[M] {
	for _, t := range r.targets {
		if k, ok := t.keyOf(owner); ok {
			return And(r.typ.Eq(t.alias()), r.id.Eq(k))
		}
	}
	return Raw[M]("1 = 0")
}

// HasMorph is whereHasMorph for one target type: the owner is an R matching scopes.
func (r *MorphToRel[M, MK, J]) HasMorph[R any, RK comparable](key Column[R, RK, J], scopes ...func(Query[R, RK]) Query[R, RK]) Cond[M] {
	sub := key.table.Query()
	if key.table.Name == r.id.table.Name {
		sub.alias = selfAlias
	}
	sub = chain(scopes)(sub.Where(key.EqCol(r.id.outer())))
	return And(r.typ.Eq(key.table.morphAlias()), Exists[M](sub))
}

// ---------------------------------------------------------------------------

// sharedRowsLimit refuses a limit in the eager-load scope of a relation
// whose related rows are shared between parents (many-to-many, through):
// one query cannot apply it per parent.
func sharedRowsLimit[R any, RK comparable](q Query[R, RK], table string) error {
	if q.limit == 0 && q.offset == 0 {
		return nil
	}
	return fmt.Errorf("orm: eager loading %s: a Limit or Offset in this relation's scope cannot apply per parent; load each parent's %s with Of instead", table, table)
}

func relQuery[R any, RK comparable](t *Table[R, RK], conn string) Query[R, RK] {
	q := t.Query()
	if conn != "" {
		q = q.On(conn)
	}
	return q
}

func chain[M any, MK comparable](scopes []func(Query[M, MK]) Query[M, MK]) func(Query[M, MK]) Query[M, MK] {
	return func(q Query[M, MK]) Query[M, MK] { return q.Scope(scopes...) }
}

func uniqueKeys[M any, MK comparable, J comparable](models []M, col Column[M, MK, J], skipZero bool) []J {
	var zero J
	seen := map[J]bool{}
	var keys []J
	for i := range models {
		k := col.Get(&models[i])
		if (skipZero && k == zero) || seen[k] {
			continue
		}
		seen[k] = true
		keys = append(keys, k)
	}
	return keys
}
