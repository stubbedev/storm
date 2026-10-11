package orm

import "context"

// Session is a table bound to a context, from Users.WithContext(ctx): its
// lookups, writes and queries run inside the transaction ctx carries,
// under its deadline and options. The table's own methods run in a
// session on the background context.
type Session[M any, K comparable] struct {
	bound
	t *Table[M, K]
}

// WithContext binds the table to ctx: Users.WithContext(ctx).Find(5).
func (t *Table[M, K]) WithContext(ctx context.Context) Session[M, K] {
	return Session[M, K]{boundTo(ctx), t}
}

func (t *Table[M, K]) session() Session[M, K] { return Session[M, K]{t: t} }

// with is s bound to ctx instead.
func (s Session[M, K]) with(ctx context.Context) Session[M, K] { return s.t.WithContext(ctx) }

// Query starts a query on the table running with the session's context.
func (s Session[M, K]) Query() Query[M, K] { return s.t.Query().WithContext(s.context()) }

func (s Session[M, K]) Where(conds ...Cond[M]) Query[M, K] { return s.Query().Where(conds...) }

func (s Session[M, K]) WhereKey(ids ...K) Query[M, K] { return s.Query().WhereKey(ids...) }

func (s Session[M, K]) WhereKeyNot(ids ...K) Query[M, K] { return s.Query().WhereKeyNot(ids...) }

func (t *Table[M, K]) All() ([]M, error) { return t.session().All() }

func (t *Table[M, K]) Find(id K) (M, error) { return t.session().Find(id) }

func (t *Table[M, K]) FindOr(id K, fn func() (M, error)) (M, error) {
	return t.session().FindOr(id, fn)
}

func (t *Table[M, K]) FindMany(ids ...K) ([]M, error) { return t.session().FindMany(ids...) }

func (t *Table[M, K]) Create(m *M) error { return t.session().Create(m) }

func (t *Table[M, K]) Save(m *M) error { return t.session().Save(m) }

func (t *Table[M, K]) Delete(m *M) error { return t.session().Delete(m) }

func (t *Table[M, K]) ForceDelete(m *M) error { return t.session().ForceDelete(m) }

func (t *Table[M, K]) Restore(m *M) error { return t.session().Restore(m) }

func (t *Table[M, K]) Touch(m *M) error { return t.session().Touch(m) }

func (t *Table[M, K]) Refresh(m *M) error { return t.session().Refresh(m) }

func (t *Table[M, K]) Fresh(m *M) (M, error) { return t.session().Fresh(m) }

func (t *Table[M, K]) Replicate(m *M, except ...AnyColumn[M]) (M, error) {
	return t.session().Replicate(m, except...)
}

func (t *Table[M, K]) Destroy(ids ...K) (int, error) { return t.session().Destroy(ids...) }

func (t *Table[M, K]) Truncate() error { return t.session().Truncate() }

func (t *Table[M, K]) SaveQuietly(m *M) error { return t.session().SaveQuietly(m) }

func (t *Table[M, K]) DeleteQuietly(m *M) error { return t.session().DeleteQuietly(m) }

func (t *Table[M, K]) ForceDeleteQuietly(m *M) error { return t.session().ForceDeleteQuietly(m) }

func (t *Table[M, K]) RestoreQuietly(m *M) error { return t.session().RestoreQuietly(m) }
