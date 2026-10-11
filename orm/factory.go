package orm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
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
// Factories are immutable; every method returns a modified copy. State
// and Sequence apply in the order they are called, each to every model;
// an invalid Count or an empty Sequence is reported by Make and Create.
type Factory[M any, K comparable] struct {
	bound
	table  *Table[M, K]
	def    func(n int) M
	seq    *atomic.Int64
	count  int
	states []func(i int, m *M)
	after  []func(context.Context, *M) error
	err    error
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

// Count sets how many models Make and Create produce; it cannot be
// negative.
func (f Factory[M, K]) Count(n int) Factory[M, K] {
	if n < 0 {
		return f.fail(fmt.Errorf("orm: a factory cannot make %d models", n))
	}
	f.count = n
	return f
}

// State applies modifications to every model.
func (f Factory[M, K]) State(fns ...func(*M)) Factory[M, K] {
	for _, fn := range fns {
		f = f.state(func(_ int, m *M) { fn(m) })
	}
	return f
}

// Sequence applies the modifications in turn, cycling: the first model gets
// fns[0], the second fns[1], ...
func (f Factory[M, K]) Sequence(fns ...func(*M)) Factory[M, K] {
	if len(fns) == 0 {
		return f.fail(errors.New("orm: a factory sequence needs at least one state"))
	}
	fns = slices.Clone(fns)
	return f.state(func(i int, m *M) { fns[i%len(fns)](m) })
}

func (f Factory[M, K]) state(fn func(i int, m *M)) Factory[M, K] {
	f.states = append(slices.Clip(f.states), fn)
	return f
}

func (f Factory[M, K]) fail(err error) Factory[M, K] {
	f.err = cmp.Or(f.err, err)
	return f
}

// AfterCreating runs fn on each model after it is inserted — e.g. to create
// related models (has / for).
func (f Factory[M, K]) AfterCreating(fn func(ctx context.Context, m *M) error) Factory[M, K] {
	f.after = append(slices.Clip(f.after), fn)
	return f
}

// Make builds models without saving them.
func (f Factory[M, K]) Make() ([]M, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]M, f.count)
	for i := range out {
		out[i] = f.def(int(f.seq.Add(1)))
		for _, s := range f.states {
			s(i, &out[i])
		}
	}
	return out, nil
}

// MakeOne builds a single unsaved model.
func (f Factory[M, K]) MakeOne() (M, error) {
	ms, err := f.Count(1).Make()
	if err != nil {
		var zero M
		return zero, err
	}
	return ms[0], nil
}

// Create builds and inserts models, firing model events.
func (f Factory[M, K]) Create() ([]M, error) {
	ctx := f.context()
	models, err := f.Make()
	if err != nil {
		return nil, err
	}
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
	ms, err := f.Count(1).Create()
	if len(ms) == 0 {
		var zero M
		return zero, err
	}
	return ms[0], err
}
