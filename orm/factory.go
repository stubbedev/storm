package orm

import (
	"context"
	"slices"
	"sync/atomic"
)

// Factory builds models for tests and seeders (Eloquent model factories):
//
//	var UserFactory = orm.NewFactory(Users.Table, func(n int) User {
//		return User{Name: fmt.Sprintf("User %d", n), Email: fmt.Sprintf("user%d@example.com", n)}
//	})
//
//	admins, err := UserFactory.Count(3).State(func(u *User) { u.Karma = 100 }).WithContext(ctx).Create()
//
// Factories are immutable; every method returns a modified copy.
type Factory[M any, K comparable] struct {
	bound
	table    *Table[M, K]
	def      func(n int) M
	seq      *atomic.Int64
	count    int
	states   []func(*M)
	sequence []func(*M)
	after    []func(context.Context, *M) error
}

// NewFactory returns a factory whose definition receives a sequence number
// that increases across every model the factory makes.
func NewFactory[M any, K comparable](t *Table[M, K], definition func(n int) M) Factory[M, K] {
	return Factory[M, K]{table: t, def: definition, seq: new(atomic.Int64), count: 1}
}

// WithContext returns the factory creating its models with ctx: inside
// the transaction ctx carries.
func (f Factory[M, K]) WithContext(ctx context.Context) Factory[M, K] {
	f.bound = boundTo(ctx)
	return f
}

// Count sets how many models Make and Create produce.
func (f Factory[M, K]) Count(n int) Factory[M, K] { f.count = n; return f }

// State applies modifications to every model.
func (f Factory[M, K]) State(fns ...func(*M)) Factory[M, K] {
	f.states = append(slices.Clip(f.states), fns...)
	return f
}

// Sequence applies the modifications in turn, cycling: the first model gets
// fns[0], the second fns[1], ...
func (f Factory[M, K]) Sequence(fns ...func(*M)) Factory[M, K] {
	f.sequence = slices.Clone(fns)
	return f
}

// AfterCreating runs fn on each model after it is inserted — e.g. to create
// related models (has / for).
func (f Factory[M, K]) AfterCreating(fn func(ctx context.Context, m *M) error) Factory[M, K] {
	f.after = append(slices.Clip(f.after), fn)
	return f
}

// Make builds models without saving them.
func (f Factory[M, K]) Make() []M {
	out := make([]M, f.count)
	for i := range out {
		out[i] = f.def(int(f.seq.Add(1)))
		for _, s := range f.states {
			s(&out[i])
		}
		if len(f.sequence) > 0 {
			f.sequence[i%len(f.sequence)](&out[i])
		}
	}
	return out
}

// MakeOne builds a single unsaved model.
func (f Factory[M, K]) MakeOne() M { return f.Count(1).Make()[0] }

// Create builds and inserts models, firing model events.
func (f Factory[M, K]) Create() ([]M, error) {
	ctx := f.context()
	models := f.Make()
	for i := range models {
		if err := f.table.WithContext(ctx).Create(&models[i]); err != nil {
			return models[:i], err
		}
		for _, fn := range f.after {
			if err := fn(ctx, &models[i]); err != nil {
				return models[:i+1], err
			}
		}
	}
	return models, nil
}

// CreateOne builds and inserts a single model.
func (f Factory[M, K]) CreateOne() (M, error) {
	ctx := f.context()
	ms, err := f.Count(1).WithContext(ctx).Create()
	if len(ms) == 0 {
		var zero M
		return zero, err
	}
	return ms[0], err
}
