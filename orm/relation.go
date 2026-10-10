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
type Loader[M, R any] interface {
	eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) error
}

// Relation is a Loader that can also be expressed as a subquery correlated
// to the parent row — which powers whereHas, withCount, withSum, ...
type Relation[M, R any] interface {
	Loader[M, R]
	correlate(scope func(Query[R]) Query[R]) Query[R]
}

// selfAlias aliases the inner table when a relation points at its own table.
const selfAlias = "laravel_reserved_0"

// ---------------------------------------------------------------------------
// Query methods using relations
// ---------------------------------------------------------------------------

// With eager-loads a relation. Scopes constrain the related query, and
// nested eager loads are just With calls inside a scope.
func (q Query[M]) With[R any](rel Loader[M, R], scopes ...func(Query[R]) Query[R]) Query[M] {
	scope := chain(scopes)
	q.eager = append(slices.Clip(q.eager), func(ctx context.Context, conn string, models []M) error {
		return rel.eagerLoad(ctx, conn, models, scope)
	})
	return q
}

// Load eager-loads a relation onto already retrieved models ($models->load()).
func Load[M, R any](ctx context.Context, models []M, rel Loader[M, R], scopes ...func(Query[R]) Query[R]) error {
	return rel.eagerLoad(ctx, "", models, chain(scopes))
}

// WithCount selects the number of related rows into a virtual column.
func (q Query[M]) WithCount[R any](rel Relation[M, R], into Column[M, int64], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.AddSelectAs(into, relAggregate[int64](rel, scopes, func(b *SQL) { b.Write("COUNT(*)") }))
}

// WithExists selects whether any related row exists into a virtual column.
func (q Query[M]) WithExists[R any](rel Relation[M, R], into Column[M, bool], scopes ...func(Query[R]) Query[R]) Query[M] {
	sub := rel.correlate(chain(scopes)).forAggregate()
	return q.AddSelectAs(into, Scalar[M, bool]{f: func(b *SQL) {
		b.Write("EXISTS (")
		sub.renderSelect(b, selectOne)
		b.Write(")")
	}})
}

func (q Query[M]) WithSum[R any, V Number](rel Relation[M, R], col Column[R, V], into Column[M, V], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.AddSelectAs(into, relAggregate[V](rel, scopes, Sum(col).build))
}

func (q Query[M]) WithAvg[R any, V Number](rel Relation[M, R], col Column[R, V], into Column[M, float64], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.AddSelectAs(into, relAggregate[float64](rel, scopes, Avg(col).build))
}

func (q Query[M]) WithMin[R, V any](rel Relation[M, R], col Column[R, V], into Column[M, V], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.AddSelectAs(into, relAggregate[V](rel, scopes, Min(col).build))
}

func (q Query[M]) WithMax[R, V any](rel Relation[M, R], col Column[R, V], into Column[M, V], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.AddSelectAs(into, relAggregate[V](rel, scopes, Max(col).build))
}

// relAggregate is COALESCE((SELECT agg FROM related WHERE correlated), zero).
func relAggregate[V, M, R any](rel Relation[M, R], scopes []func(Query[R]) Query[R], agg frag) Subquery[V] {
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
func Has[M, R any](rel Relation[M, R], scopes ...func(Query[R]) Query[R]) Cond[M] {
	return Exists[M](rel.correlate(chain(scopes)))
}

func DoesntHave[M, R any](rel Relation[M, R], scopes ...func(Query[R]) Query[R]) Cond[M] {
	return Not(Has(rel, scopes...))
}

func (q Query[M]) WhereHas[R any](rel Relation[M, R], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.Where(Has(rel, scopes...))
}

func (q Query[M]) OrWhereHas[R any](rel Relation[M, R], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.OrWhere(Has(rel, scopes...))
}

func (q Query[M]) WhereDoesntHave[R any](rel Relation[M, R], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.Where(DoesntHave(rel, scopes...))
}

func (q Query[M]) OrWhereDoesntHave[R any](rel Relation[M, R], scopes ...func(Query[R]) Query[R]) Query[M] {
	return q.OrWhere(DoesntHave(rel, scopes...))
}

// WhereRelation is whereHas with plain conditions on the related model.
func (q Query[M]) WhereRelation[R any](rel Relation[M, R], conds ...Cond[R]) Query[M] {
	return q.WhereHas(rel, func(r Query[R]) Query[R] { return r.Where(conds...) })
}

// HasCount is has('posts', '>=', 3): compares the related row count.
func (q Query[M]) HasCount[R any](rel Relation[M, R], op Op, n int64, scopes ...func(Query[R]) Query[R]) Query[M] {
	sub := relAggregate[int64](rel, scopes, func(b *SQL) { b.Write("COUNT(*)") })
	return q.Where(Cond[M]{f: func(b *SQL) { compare(b, sub, op, n) }})
}

// ---------------------------------------------------------------------------
// HasMany / HasOne / MorphMany / MorphOne
// ---------------------------------------------------------------------------

type hasBase[M, R any, K comparable] struct {
	related    *Table[R]
	local      Column[M, K]
	foreign    Column[R, K]
	morphType  *Column[R, string] // set for MorphOne/MorphMany
	scopes     []func(Query[R]) Query[R]
	attributes []Assignment[R]
	inverse    func(child *R, parent *M)
}

func (h hasBase[M, R, K]) constrain(q Query[R]) Query[R] {
	if h.morphType != nil {
		q = q.Where(h.morphType.Eq(h.local.table.morphAlias()))
	}
	return q.Scope(h.scopes...)
}

func (h hasBase[M, R, K]) load(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) (map[K][]R, error) {
	keys := uniqueKeys(parents, h.local, false)
	if len(keys) == 0 {
		return nil, nil
	}
	q := scope(h.constrain(relQuery(h.related, conn).Where(h.foreign.In(keys...))))
	if q.limit > 0 || q.offset > 0 {
		q.perParent = h.foreign
	}
	children, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}
	byKey := map[K][]R{}
	for i := range children {
		k := h.foreign.Get(&children[i])
		byKey[k] = append(byKey[k], children[i])
	}
	return byKey, nil
}

func (h hasBase[M, R, K]) correlate(scope func(Query[R]) Query[R]) Query[R] {
	q := h.related.Query()
	if h.related.Name == h.local.table.Name {
		q.alias = selfAlias
	}
	return scope(h.constrain(q.Where(h.foreign.EqCol(h.local.outer()))))
}

// Of is the relationship query for one parent: UserPosts.Of(&u) is $user->posts().
func (h hasBase[M, R, K]) Of(parent *M) Query[R] {
	return h.constrain(h.related.Query().Where(h.foreign.Eq(h.local.Get(parent))))
}

// Create sets the foreign key (and morph type) on child and inserts it.
func (h hasBase[M, R, K]) Create(ctx context.Context, parent *M, child *R) error {
	h.link(parent, child)
	return h.related.Create(ctx, child)
}

// Save sets the foreign key on child and saves it.
func (h hasBase[M, R, K]) Save(ctx context.Context, parent *M, child *R) error {
	h.link(parent, child)
	return h.related.Save(ctx, child)
}

func (h hasBase[M, R, K]) link(parent *M, child *R) {
	fill(child, h.attributes)
	*h.foreign.Ptr(child) = h.local.Get(parent)
	if h.morphType != nil {
		*h.morphType.Ptr(child) = h.local.table.morphAlias()
	}
}

// HasManyRel is a one-to-many relation (hasMany / morphMany).
type HasManyRel[M, R any, K comparable] struct {
	hasBase[M, R, K]
	set func(*M, []R)
}

// HasMany: parent M has many R where R.foreign = M.local.
//
//	var UserPosts = orm.HasMany(Users.ID, Posts.UserID,
//		func(u *User, ps []Post) { u.Posts = ps })
//
// The related table is foreign's, so it cannot disagree with the columns.
func HasMany[M, R any, K comparable](local Column[M, K], foreign Column[R, K], set func(*M, []R)) *HasManyRel[M, R, K] {
	return &HasManyRel[M, R, K]{hasBase[M, R, K]{related: foreign.table, local: local, foreign: foreign}, set}
}

// MorphMany: parent M has many polymorphic R (R.morphID = M.local AND R.morphType = M's alias).
func MorphMany[M, R any, K comparable](local Column[M, K], morphID Column[R, K], morphType Column[R, string], set func(*M, []R)) *HasManyRel[M, R, K] {
	r := HasMany(local, morphID, set)
	r.morphType = &morphType
	return r
}

func (r *HasManyRel[M, R, K]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) error {
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

// Scoped returns the relation with extra constraints (a "scoped relationship").
func (r *HasManyRel[M, R, K]) Scoped(scopes ...func(Query[R]) Query[R]) *HasManyRel[M, R, K] {
	c := *r
	c.scopes = append(slices.Clip(c.scopes), scopes...)
	return &c
}

// WithAttributes sets attributes on models created through the relation.
func (r *HasManyRel[M, R, K]) WithAttributes(attrs ...Assignment[R]) *HasManyRel[M, R, K] {
	c := *r
	c.attributes = append(slices.Clip(c.attributes), attrs...)
	return &c
}

// Chaperone sets the inverse (child -> parent) when eager loading.
func (r *HasManyRel[M, R, K]) Chaperone(set func(child *R, parent *M)) *HasManyRel[M, R, K] {
	c := *r
	c.inverse = set
	return &c
}

// SaveMany saves each child through the relation.
func (r *HasManyRel[M, R, K]) SaveMany(ctx context.Context, parent *M, children []R) error {
	for i := range children {
		if err := r.Save(ctx, parent, &children[i]); err != nil {
			return err
		}
	}
	return nil
}

// HasOneRel is a one-to-one relation (hasOne / morphOne / ofMany).
type HasOneRel[M, R any, K comparable] struct {
	hasBase[M, R, K]
	set    func(*M, *R)
	better func(a, b *R) bool // ofMany: whether a beats b
}

func HasOne[M, R any, K comparable](local Column[M, K], foreign Column[R, K], set func(*M, *R)) *HasOneRel[M, R, K] {
	return &HasOneRel[M, R, K]{related: foreign.table, local: local, foreign: foreign, set: set}
}

func MorphOne[M, R any, K comparable](local Column[M, K], morphID Column[R, K], morphType Column[R, string], set func(*M, *R)) *HasOneRel[M, R, K] {
	r := HasOne(local, morphID, set)
	r.morphType = &morphType
	return r
}

// OfMany picks one of many related rows by the max (or min) of col —
// latestOfMany / oldestOfMany / ofMany('price', 'max').
func (r *HasOneRel[M, R, K]) OfMany[V interface {
	~int | ~int64 | ~float64 | ~string
}](col Column[R, V], useMax bool) *HasOneRel[M, R, K] {
	c := *r
	c.better = func(a, b *R) bool {
		if useMax {
			return col.Get(a) > col.Get(b)
		}
		return col.Get(a) < col.Get(b)
	}
	return &c
}

func (r *HasOneRel[M, R, K]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) error {
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
type BelongsToRel[M, R any, K comparable] struct {
	related *Table[R]
	foreign Column[M, K]
	owner   Column[R, K]
	set     func(*M, *R)
	def     func(*M) *R
	scopes  []func(Query[R]) Query[R]
}

// BelongsTo: child M points at one R where M.foreign = R.owner.
func BelongsTo[M, R any, K comparable](foreign Column[M, K], owner Column[R, K], set func(*M, *R)) *BelongsToRel[M, R, K] {
	return &BelongsToRel[M, R, K]{related: owner.table, foreign: foreign, owner: owner, set: set}
}

// WithDefault supplies a model when the owner is missing (withDefault()).
func (r *BelongsToRel[M, R, K]) WithDefault(fn func(*M) *R) *BelongsToRel[M, R, K] {
	c := *r
	c.def = fn
	return &c
}

func (r *BelongsToRel[M, R, K]) eagerLoad(ctx context.Context, conn string, children []M, scope func(Query[R]) Query[R]) error {
	byKey := map[K]*R{}
	if keys := uniqueKeys(children, r.foreign, true); len(keys) > 0 {
		owners, err := scope(relQuery(r.related, conn).Where(r.owner.In(keys...)).Scope(r.scopes...)).Get(ctx)
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

func (r *BelongsToRel[M, R, K]) correlate(scope func(Query[R]) Query[R]) Query[R] {
	q := r.related.Query()
	if r.related.Name == r.foreign.table.Name {
		q.alias = selfAlias
	}
	return scope(q.Where(r.owner.EqCol(r.foreign.outer())).Scope(r.scopes...))
}

// Of is the relationship query for one child ($post->author()).
func (r *BelongsToRel[M, R, K]) Of(child *M) Query[R] {
	return r.related.Query().Where(r.owner.Eq(r.foreign.Get(child))).Scope(r.scopes...)
}

// Associate points child at owner.
func (r *BelongsToRel[M, R, K]) Associate(child *M, owner *R) {
	*r.foreign.Ptr(child) = r.owner.Get(owner)
	r.set(child, owner)
}

// Dissociate clears child's foreign key.
func (r *BelongsToRel[M, R, K]) Dissociate(child *M) {
	var zero K
	*r.foreign.Ptr(child) = zero
	r.set(child, nil)
}

// Is matches children of owner (whereBelongsTo).
func (r *BelongsToRel[M, R, K]) Is(owner *R) Cond[M] { return r.foreign.Eq(r.owner.Get(owner)) }

// Touches makes saving or deleting a child update the owner's updated-at
// ($touches), except under WithoutTimestamps.
func (r *BelongsToRel[M, R, K]) Touches() {
	if r.related.UpdatedAt == "" {
		return
	}
	touch := func(ctx context.Context, m *M) error {
		if !timestamps(ctx) {
			return nil
		}
		_, err := r.related.Query().Where(r.owner.Eq(r.foreign.Get(m))).Update(ctx)
		return err
	}
	r.foreign.table.On(Saved, touch)
	r.foreign.table.On(Deleted, touch)
}

// ---------------------------------------------------------------------------
// BelongsToMany / MorphToMany / MorphedByMany
// ---------------------------------------------------------------------------

// BelongsToManyRel is a many-to-many relation through pivot model P.
type BelongsToManyRel[M, R, P any, K, RK comparable] struct {
	related      *Table[R]
	pivot        *Table[P]
	parentKey    Column[M, K]
	relatedKey   Column[R, RK]
	pivotParent  Column[P, K]
	pivotRelated Column[P, RK]
	pivotType    *Column[P, string] // morph pivots
	typeValue    string
	set          func(*M, []R)
	setPivot     func(*R, P)
	pivotWhere   []Cond[P]
	scopes       []func(Query[R]) Query[R]
}

// BelongsToMany: M and R are linked through pivot rows P where
// P.pivotParent = M.parentKey and P.pivotRelated = R.relatedKey.
// The pivot is a regular (generated) model, so pivot columns are typed;
// the related and pivot tables are their columns' tables.
func BelongsToMany[M, R, P any, K, RK comparable](
	parentKey Column[M, K], pivotParent Column[P, K],
	relatedKey Column[R, RK], pivotRelated Column[P, RK],
	set func(*M, []R),
) *BelongsToManyRel[M, R, P, K, RK] {
	return &BelongsToManyRel[M, R, P, K, RK]{
		related: relatedKey.table, pivot: pivotParent.table, parentKey: parentKey, relatedKey: relatedKey,
		pivotParent: pivotParent, pivotRelated: pivotRelated, set: set,
	}
}

// MorphToMany: polymorphic many-to-many from the owning side (Post -> Tags
// via taggables(taggable_id, taggable_type, tag_id)).
func MorphToMany[M, R, P any, K, RK comparable](
	parentKey Column[M, K], pivotMorphID Column[P, K], pivotMorphType Column[P, string],
	relatedKey Column[R, RK], pivotRelated Column[P, RK],
	set func(*M, []R),
) *BelongsToManyRel[M, R, P, K, RK] {
	r := BelongsToMany(parentKey, pivotMorphID, relatedKey, pivotRelated, set)
	r.pivotType, r.typeValue = &pivotMorphType, parentKey.table.morphAlias()
	return r
}

// MorphedByMany: the inverse (Tag -> Posts via taggables).
func MorphedByMany[M, R, P any, K, RK comparable](
	parentKey Column[M, K], pivotParent Column[P, K],
	relatedKey Column[R, RK], pivotMorphID Column[P, RK], pivotMorphType Column[P, string],
	set func(*M, []R),
) *BelongsToManyRel[M, R, P, K, RK] {
	r := BelongsToMany(parentKey, pivotParent, relatedKey, pivotMorphID, set)
	r.pivotType, r.typeValue = &pivotMorphType, relatedKey.table.morphAlias()
	return r
}

// WithPivot hands each loaded related model its pivot row (withPivot / as).
func (r *BelongsToManyRel[M, R, P, K, RK]) WithPivot(set func(*R, P)) *BelongsToManyRel[M, R, P, K, RK] {
	c := *r
	c.setPivot = set
	return &c
}

// WherePivot constrains the pivot rows (wherePivot, wherePivotIn, ...).
func (r *BelongsToManyRel[M, R, P, K, RK]) WherePivot(conds ...Cond[P]) *BelongsToManyRel[M, R, P, K, RK] {
	c := *r
	c.pivotWhere = append(slices.Clip(c.pivotWhere), conds...)
	return &c
}

func (r *BelongsToManyRel[M, R, P, K, RK]) Scoped(scopes ...func(Query[R]) Query[R]) *BelongsToManyRel[M, R, P, K, RK] {
	c := *r
	c.scopes = append(slices.Clip(c.scopes), scopes...)
	return &c
}

func (r *BelongsToManyRel[M, R, P, K, RK]) pivotConds() []Cond[P] {
	conds := slices.Clone(r.pivotWhere)
	if r.pivotType != nil {
		conds = append(conds, r.pivotType.Eq(r.typeValue))
	}
	return conds
}

func (r *BelongsToManyRel[M, R, P, K, RK]) pivotQuery(conn string, parent K) Query[P] {
	return relQuery(r.pivot, conn).Where(r.pivotParent.Eq(parent)).Where(r.pivotConds()...)
}

func (r *BelongsToManyRel[M, R, P, K, RK]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) error {
	keys := uniqueKeys(parents, r.parentKey, false)
	if len(keys) == 0 {
		return nil
	}
	pivots, err := relQuery(r.pivot, conn).Where(r.pivotParent.In(keys...)).Where(r.pivotConds()...).Get(ctx)
	if err != nil {
		return err
	}
	byRelated := map[RK][]P{}
	var rks []RK
	for i := range pivots {
		rk := r.pivotRelated.Get(&pivots[i])
		if _, seen := byRelated[rk]; !seen {
			rks = append(rks, rk)
		}
		byRelated[rk] = append(byRelated[rk], pivots[i])
	}
	out := map[K][]R{}
	if len(rks) > 0 {
		q := scope(relQuery(r.related, conn).Where(r.relatedKey.In(rks...)).Scope(r.scopes...))
		if err := sharedRowsLimit(q, r.related.Name); err != nil {
			return err
		}
		related, err := q.Get(ctx)
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

func (r *BelongsToManyRel[M, R, P, K, RK]) correlate(scope func(Query[R]) Query[R]) Query[R] {
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
func (r *BelongsToManyRel[M, R, P, K, RK]) Of(parent *M) Query[R] {
	return r.related.Query().
		Join(r.pivot, r.pivotRelated.EqCol(r.relatedKey)).
		WhereOf(r.pivotParent.Eq(r.parentKey.Get(parent))).
		WhereOf(r.pivotConds()...).
		Scope(r.scopes...)
}

// AttachedTo matches parents attached to related (whereAttachedTo).
func (r *BelongsToManyRel[M, R, P, K, RK]) AttachedTo(related *R) Cond[M] {
	rk := r.relatedKey.Get(related)
	return Has[M, R](r, func(q Query[R]) Query[R] { return q.Where(r.relatedKey.Eq(rk)) })
}

// Attach inserts pivot rows for parent; parent key and morph type are filled
// in, pivot timestamps maintained. Extra pivot columns come from the P values.
func (r *BelongsToManyRel[M, R, P, K, RK]) Attach(ctx context.Context, parent *M, pivots ...P) error {
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
	return r.pivot.Query().Insert(ctx, pivots...)
}

// AttachIDs attaches related keys without extra pivot data.
func (r *BelongsToManyRel[M, R, P, K, RK]) AttachIDs(ctx context.Context, parent *M, ids ...RK) error {
	pivots := make([]P, len(ids))
	for i, id := range ids {
		*r.pivotRelated.Ptr(&pivots[i]) = id
	}
	return r.Attach(ctx, parent, pivots...)
}

// Detach removes pivot rows for ids, or all of parent's when none are given.
func (r *BelongsToManyRel[M, R, P, K, RK]) Detach(ctx context.Context, parent *M, ids ...RK) (int64, error) {
	q := r.pivotQuery("", r.parentKey.Get(parent))
	if len(ids) > 0 {
		q = q.Where(r.pivotRelated.In(ids...))
	}
	return q.hardDelete(ctx)
}

// SyncResult reports what Sync and Toggle changed.
type SyncResult[RK comparable] struct{ Attached, Detached []RK }

// Sync makes ids exactly the attached set.
func (r *BelongsToManyRel[M, R, P, K, RK]) Sync(ctx context.Context, parent *M, ids ...RK) (SyncResult[RK], error) {
	return r.sync(ctx, parent, ids, true)
}

// SyncWithoutDetaching attaches missing ids and leaves others alone.
func (r *BelongsToManyRel[M, R, P, K, RK]) SyncWithoutDetaching(ctx context.Context, parent *M, ids ...RK) (SyncResult[RK], error) {
	return r.sync(ctx, parent, ids, false)
}

func (r *BelongsToManyRel[M, R, P, K, RK]) sync(ctx context.Context, parent *M, ids []RK, detach bool) (SyncResult[RK], error) {
	var res SyncResult[RK]
	current, err := r.pivotQuery("", r.parentKey.Get(parent)).Pluck(ctx, r.pivotRelated)
	if err != nil {
		return res, err
	}
	seen := make(map[RK]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
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
		if _, err := r.Detach(ctx, parent, res.Detached...); err != nil {
			return res, err
		}
	}
	if len(res.Attached) > 0 {
		err = r.AttachIDs(ctx, parent, res.Attached...)
	}
	return res, err
}

// Toggle detaches attached ids and attaches the rest.
func (r *BelongsToManyRel[M, R, P, K, RK]) Toggle(ctx context.Context, parent *M, ids ...RK) (SyncResult[RK], error) {
	var res SyncResult[RK]
	current, err := r.pivotQuery("", r.parentKey.Get(parent)).Pluck(ctx, r.pivotRelated)
	if err != nil {
		return res, err
	}
	seen := make(map[RK]bool, len(ids))
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
		if _, err := r.Detach(ctx, parent, res.Detached...); err != nil {
			return res, err
		}
	}
	if len(res.Attached) > 0 {
		err = r.AttachIDs(ctx, parent, res.Attached...)
	}
	return res, err
}

// UpdateExistingPivot updates the pivot row linking parent and id.
func (r *BelongsToManyRel[M, R, P, K, RK]) UpdateExistingPivot(ctx context.Context, parent *M, id RK, sets ...Assignment[P]) (int64, error) {
	return r.pivotQuery("", r.parentKey.Get(parent)).Where(r.pivotRelated.Eq(id)).Update(ctx, sets...)
}

// ---------------------------------------------------------------------------
// HasManyThrough / HasOneThrough
// ---------------------------------------------------------------------------

type throughBase[M, T, R any, K, TK comparable] struct {
	related     *Table[R]
	through     *Table[T]
	localKey    Column[M, K]  // on M
	firstKey    Column[T, K]  // on T, pointing at M
	secondLocal Column[T, TK] // on T
	secondKey   Column[R, TK] // on R, pointing at T
}

func (h throughBase[M, T, R, K, TK]) load(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) (map[K][]R, error) {
	keys := uniqueKeys(parents, h.localKey, false)
	if len(keys) == 0 {
		return nil, nil
	}
	throughs, err := relQuery(h.through, conn).Where(h.firstKey.In(keys...)).Get(ctx)
	if err != nil || len(throughs) == 0 {
		return nil, err
	}
	parentsOf := map[TK][]K{}
	var tks []TK
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
	related, err := q.Get(ctx)
	if err != nil {
		return nil, err
	}
	out := map[K][]R{}
	for i := range related {
		for _, k := range parentsOf[h.secondKey.Get(&related[i])] {
			out[k] = append(out[k], related[i])
		}
	}
	return out, nil
}

func (h throughBase[M, T, R, K, TK]) joined(q Query[R]) Query[R] {
	q = q.Join(h.through, h.secondLocal.EqCol(h.secondKey))
	if t := h.through; t.softDeletes() {
		q = q.WhereOf(Cond[T]{f: func(b *SQL) { b.Col(t.Name, t.DeletedAt); b.Write(" IS NULL") }})
	}
	return q
}

func (h throughBase[M, T, R, K, TK]) correlate(scope func(Query[R]) Query[R]) Query[R] {
	return scope(h.joined(h.related.Query()).WhereOf(h.firstKey.EqCol(h.localKey.outer())))
}

// Of is the relationship query for one parent.
func (h throughBase[M, T, R, K, TK]) Of(parent *M) Query[R] {
	return h.joined(h.related.Query()).WhereOf(h.firstKey.Eq(h.localKey.Get(parent)))
}

// HasManyThroughRel reaches R through an intermediate T.
type HasManyThroughRel[M, T, R any, K, TK comparable] struct {
	throughBase[M, T, R, K, TK]
	set func(*M, []R)
}

// HasManyThrough: M -> T (T.firstKey = M.localKey) -> R (R.secondKey = T.secondLocal).
//
//	var CountryPosts = orm.HasManyThrough(
//		Countries.ID, Users.CountryID, Users.ID, Posts.UserID, set)
func HasManyThrough[M, T, R any, K, TK comparable](
	localKey Column[M, K], firstKey Column[T, K],
	secondLocal Column[T, TK], secondKey Column[R, TK],
	set func(*M, []R),
) *HasManyThroughRel[M, T, R, K, TK] {
	return &HasManyThroughRel[M, T, R, K, TK]{through(localKey, firstKey, secondLocal, secondKey), set}
}

// through is the shared part of the through relations; the tables are
// their key columns' tables.
func through[M, T, R any, K, TK comparable](localKey Column[M, K], firstKey Column[T, K], secondLocal Column[T, TK], secondKey Column[R, TK]) throughBase[M, T, R, K, TK] {
	return throughBase[M, T, R, K, TK]{secondKey.table, firstKey.table, localKey, firstKey, secondLocal, secondKey}
}

func (r *HasManyThroughRel[M, T, R, K, TK]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) error {
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
type HasOneThroughRel[M, T, R any, K, TK comparable] struct {
	throughBase[M, T, R, K, TK]
	set func(*M, *R)
}

func HasOneThrough[M, T, R any, K, TK comparable](
	localKey Column[M, K], firstKey Column[T, K],
	secondLocal Column[T, TK], secondKey Column[R, TK],
	set func(*M, *R),
) *HasOneThroughRel[M, T, R, K, TK] {
	return &HasOneThroughRel[M, T, R, K, TK]{through(localKey, firstKey, secondLocal, secondKey), set}
}

func (r *HasOneThroughRel[M, T, R, K, TK]) eagerLoad(ctx context.Context, conn string, parents []M, scope func(Query[R]) Query[R]) error {
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
type MorphTarget[K comparable] interface {
	alias() string
	load(ctx context.Context, conn string, keys []K) (map[K]any, error)
	keyOf(owner any) (K, bool)
}

type morphTarget[R any, K comparable] struct{ key Column[R, K] }

// Target declares a MorphTo target by its key column: orm.Target(Posts.ID).
func Target[R any, K comparable](key Column[R, K]) MorphTarget[K] { return morphTarget[R, K]{key} }

func (t morphTarget[R, K]) alias() string { return t.key.table.morphAlias() }

func (t morphTarget[R, K]) load(ctx context.Context, conn string, keys []K) (map[K]any, error) {
	rows, err := relQuery(t.key.table, conn).Where(t.key.In(keys...)).Get(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[K]any, len(rows))
	for i := range rows {
		out[t.key.Get(&rows[i])] = &rows[i]
	}
	return out, nil
}

func (t morphTarget[R, K]) keyOf(owner any) (K, bool) {
	if r, ok := owner.(*R); ok {
		return t.key.Get(r), true
	}
	var zero K
	return zero, false
}

// MorphToRel is the polymorphic inverse: the owner may be any of several
// models, so it is delivered as `any` holding a *Post, *Video, ...
type MorphToRel[M any, K comparable] struct {
	id      Column[M, K]
	typ     Column[M, string]
	set     func(*M, any)
	targets []MorphTarget[K]
}

// MorphTo: comments(commentable_id, commentable_type) -> Post | Video.
func MorphTo[M any, K comparable](id Column[M, K], typ Column[M, string], set func(*M, any), targets ...MorphTarget[K]) *MorphToRel[M, K] {
	return &MorphToRel[M, K]{id, typ, set, targets}
}

func (r *MorphToRel[M, K]) eagerLoad(ctx context.Context, conn string, children []M, _ func(Query[any]) Query[any]) error {
	byType := map[string][]K{}
	for i := range children {
		t, k := r.typ.Get(&children[i]), r.id.Get(&children[i])
		if !slices.Contains(byType[t], k) {
			byType[t] = append(byType[t], k)
		}
	}
	loaded := map[string]map[K]any{}
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
func (r *MorphToRel[M, K]) Associate(child *M, owner any) bool {
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
func (r *MorphToRel[M, K]) Is(owner any) Cond[M] {
	for _, t := range r.targets {
		if k, ok := t.keyOf(owner); ok {
			return And(r.typ.Eq(t.alias()), r.id.Eq(k))
		}
	}
	return Raw[M]("1 = 0")
}

// HasMorph is whereHasMorph for one target type: the owner is an R matching scopes.
func (r *MorphToRel[M, K]) HasMorph[R any](key Column[R, K], scopes ...func(Query[R]) Query[R]) Cond[M] {
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
func sharedRowsLimit[R any](q Query[R], table string) error {
	if q.limit == 0 && q.offset == 0 {
		return nil
	}
	return fmt.Errorf("orm: eager loading %s: a Limit or Offset in this relation's scope cannot apply per parent; load each parent's %s with Of instead", table, table)
}

func relQuery[R any](t *Table[R], conn string) Query[R] {
	q := t.Query()
	if conn != "" {
		q = q.On(conn)
	}
	return q
}

func chain[M any](scopes []func(Query[M]) Query[M]) func(Query[M]) Query[M] {
	return func(q Query[M]) Query[M] { return q.Scope(scopes...) }
}

func uniqueKeys[M any, K comparable](models []M, col Column[M, K], skipZero bool) []K {
	var zero K
	seen := map[K]bool{}
	var keys []K
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
