package models_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
)

func TestScalarExpressions(t *testing.T) {
	ctx := setup(t)
	get := func(q orm.Query[User]) []string { return names(must(q.OrderBy(Users.ID.Asc()).Get(ctx))) }
	fortyTwo := orm.Val[User](42)

	eq(t, "neCol", get(Users.Where(Users.Karma.NeCol(fortyTwo))), []string{"Bob", "Carol", "Dave"})
	eq(t, "gtCol", get(Users.Where(Users.Karma.GtCol(fortyTwo))), []string{"Carol"})
	eq(t, "gteCol", get(Users.Where(Users.Karma.GteCol(fortyTwo))), []string{"Alice", "Carol"})
	eq(t, "ltCol", get(Users.Where(Users.Karma.LtCol(fortyTwo))), []string{"Bob", "Dave"})
	eq(t, "lteCol", get(Users.Where(Users.Karma.LteCol(fortyTwo))), []string{"Alice", "Bob", "Dave"})
	eq(t, "cmp", get(Users.Where(Users.Karma.Cmp(orm.Gt, fortyTwo))), []string{"Carol"})
	_, err := Users.Where(Users.Karma.Cmp("<=>", fortyTwo)).Get(ctx)
	eq(t, "unknown operator refused", err != nil && strings.Contains(err.Error(), "unknown comparison operator"), true)

	eq(t, "notLike", get(Users.Where(Users.Name.NotLike("%ar%"))), []string{"Alice", "Bob", "Dave"})
	eq(t, "notIn", get(Users.Where(Users.ID.NotIn(1, 2))), []string{"Carol", "Dave"})
	eq(t, "notInSub", get(Users.Where(Users.ID.NotInSub(Posts.Query().Subquery(Posts.UserID)))), []string{"Dave"})
	eq(t, "notBetween", get(Users.Where(Users.Karma.NotBetween(10, 50))), []string{"Carol", "Dave"})
	eq(t, "notExists", get(Users.Where(orm.NotExists[User](Posts.Where(Posts.UserID.EqCol(Users.ID))))), []string{"Dave"})
	eq(t, "allOf", get(Users.Where(orm.AllOf(orm.Like, "%e%", Users.Name, Users.Email))), []string{"Alice", "Dave"})
	eq(t, "allOf negates every operator", get(Users.Where(orm.AllOf(orm.Gte, 17, Users.Karma))), []string{"Alice", "Bob", "Carol"})
	_, err = Users.Where(orm.AnyOf[User]("~", "x", Users.Name)).Get(ctx)
	eq(t, "anyOf unknown operator refused", err != nil && strings.Contains(err.Error(), "unknown comparison operator"), true)
	eq(t, "allOf ilike", get(Users.Where(orm.AllOf(orm.ILike, "%A%", Users.Name, Users.Email))), []string{"Alice", "Carol", "Dave"})

	byLower := must(Users.Query().OrderBy(orm.Lower(Users.Name).Desc()).Get(ctx))
	eq(t, "expression desc", names(byLower), []string{"Dave", "Carol", "Bob", "Alice"})
	eq(t, "expression asc", names(must(Users.Query().OrderBy(orm.Lower(Users.Name).Asc()).Get(ctx))), []string{"Alice", "Bob", "Carol", "Dave"})
	eq(t, "rawOrder", must(Users.Query().OrderBy(orm.RawOrder[User]("karma DESC")).First(ctx)).Name, "Carol")

	eq(t, "countDistinct", must(Posts.Query().Value(ctx, orm.CountDistinct(Posts.UserID))), int64(5))
	eq(t, "min", must(Users.Query().Min(ctx, Users.Karma)), 3)
	eq(t, "lower", must(Users.Where(Users.ID.Eq(1)).Value(ctx, orm.Lower(Users.Name))), "alice")
	eq(t, "coalesce", must(Users.Where(Users.ID.Eq(4)).Value(ctx, orm.Coalesce(Users.Name, "nobody"))), "Dave")
	eq(t, "minus", get(Users.Where(orm.Minus(Users.Karma, 2).Eq(1))), []string{"Dave"})
	eq(t, "day", must(Posts.Where(orm.Day(Posts.CreatedAt).Eq(10)).Pluck(ctx, Posts.Title)), []string{"Generics in Go 1.27"})
	eq(t, "time", must(Posts.Where(orm.Time(Posts.CreatedAt).Eq("08:00:00")).Count(ctx)), int64(7))
	eq(t, "now", must(Posts.Where(Posts.CreatedAt.LtCol(orm.Now[Post]())).Count(ctx)), int64(7))
}

func TestQueryShaping(t *testing.T) {
	ctx := setup(t)
	get := func(q orm.Query[User]) []string { return names(must(q.Get(ctx))) }
	byID := Users.Query().OrderBy(Users.ID.Asc())

	eq(t, "orWhereNot", get(byID.Where(Users.Name.Eq("Alice")).OrWhereNot(Users.Active.Eq(true))), []string{"Alice", "Carol"})
	eq(t, "whereKeyNot", get(byID.WhereKeyNot(1, 2)), []string{"Carol", "Dave"})
	eq(t, "take and skip", get(byID.Skip(1).Take(2)), []string{"Bob", "Carol"})
	eq(t, "forPage", get(byID.ForPage(2, 2)), []string{"Carol", "Dave"})
	eq(t, "oldest", must(Users.Query().Oldest().First(ctx)).Name, "Alice")
	eq(t, "oldest by column", must(Users.Query().Oldest(Users.Karma).First(ctx)).Name, "Dave")
	eq(t, "all", len(must(Users.All(ctx))), 4)

	narrow := must(byID.Select(Users.ID).AddSelect(Users.Name).First(ctx))
	eq(t, "addSelect to a selection", fmt.Sprintf("%v %v", narrow.Name, narrow.Email == ""), "Alice true")
	widened := must(byID.AddSelect(Users.Karma).First(ctx))
	eq(t, "addSelect keeps every column", fmt.Sprintf("%v %v", widened.Email, widened.Karma), "alice@example.com 42")

	eq(t, "rightJoin", must(Posts.Query().RightJoin(Users, Users.ID.EqCol(Posts.UserID)).Count(ctx)), int64(7))
	eq(t, "crossJoin", must(Countries.Query().CrossJoin(Roles).Count(ctx)), int64(6))
	eq(t, "leftJoinSub", must(Users.Query().
		LeftJoinSub(Posts.Where(Posts.Published.Eq(true)), "published", Posts.UserID.EqCol(Users.ID)).
		Count(ctx)), int64(5))
	eq(t, "orderByOf", get(Users.Query().
		Join(Countries, Countries.ID.EqCol(Users.CountryID)).
		OrderByOf(Countries.Name.Desc()).
		OrderBy(Users.Name.Asc())), []string{"Carol", "Dave", "Alice", "Bob"})
	eq(t, "orHaving", must(Posts.Query().
		GroupBy(Posts.UserID).
		Having(orm.Count[Post]().Gt(2)).
		OrHaving(orm.Sum(Posts.Views).Gt(250)).
		OrderBy(Posts.UserID.Asc()).
		Pluck(ctx, Posts.UserID)), []int64{1, 3})

	ok(orm.Transaction(ctx, func(ctx context.Context) error {
		eq(t, "inTransaction", orm.InTransaction(ctx), true)
		eq(t, "sharedLock", len(must(Users.Where(Users.ID.Eq(1)).SharedLock().Get(ctx))), 1)
		return nil
	}))
	eq(t, "not inTransaction", orm.InTransaction(ctx), false)
	ok(orm.RetryingTransactionOn(ctx, orm.DefaultConnection, 2, func(ctx context.Context) error {
		eq(t, "retryingTransactionOn", orm.InTransaction(ctx), true)
		return nil
	}))

	stmt, args := rendered(Users.Where(Users.ID.Eq(1)).ToSQL())
	eq(t, "toSQL", strings.Contains(stmt, "users") && len(args) == 1, true)

	_, err := orm.From("no_such_table").Get(ctx)
	var qe *orm.QueryError
	eq(t, "queryError", errors.As(err, &qe) && errors.Unwrap(qe) != nil && strings.Contains(qe.SQL, "no_such_table"), true)

	first := must(Users.Query().CursorPaginate(ctx, 3, ""))
	eq(t, "cursor by key", names(first.Items), []string{"Alice", "Bob", "Carol"})
	eq(t, "cursor by key next", names(must(Users.Query().CursorPaginate(ctx, 3, first.NextCursor)).Items), []string{"Dave"})
}

func TestRelationOptions(t *testing.T) {
	ctx := setup(t)
	alice := must(Users.Find(ctx, 1))
	get := func(q orm.Query[User]) []string { return names(must(q.OrderBy(Users.ID.Asc()).Get(ctx))) }

	stats := must(Users.Query().
		WithAvg(UserPosts, Posts.Views, Users.AvgPostViews).
		OrderBy(Users.ID.Asc()).
		Get(ctx))
	eq(t, "withAvg", fmt.Sprintf("%.1f %.1f", stats[0].AvgPostViews, stats[1].AvgPostViews), "213.3 150.0")
	minViews := must(Users.Query().WithMin(UserPosts, Posts.Views, Users.PostViews).OrderBy(Users.ID.Asc()).Get(ctx))
	eq(t, "withMin", minViews[0].PostViews, int64(12))
	maxViews := must(Users.Query().WithMax(UserPosts, Posts.Views, Users.PostViews).OrderBy(Users.ID.Asc()).Get(ctx))
	eq(t, "withMax", maxViews[0].PostViews, int64(540))

	eq(t, "orWhereDoesntHave", get(Users.Where(Users.Name.Eq("Alice")).OrWhereDoesntHave(UserPosts)), []string{"Alice", "Dave"})

	published := UserPosts.Scoped(Published)
	eq(t, "scoped hasMany", must(published.Of(&alice).Count(ctx)), int64(2))
	counted := must(Users.Query().WithCount(published, Users.PostsCount).OrderBy(Users.ID.Asc()).First(ctx))
	eq(t, "scoped hasMany eager", counted.PostsCount, int64(2))

	draft := Post{Title: "through relation", Published: true}
	ok(UserPosts.WithAttributes(Posts.Published.Set(false), Posts.Views.Set(7)).Create(ctx, &alice, &draft))
	eq(t, "withAttributes", fmt.Sprintf("%v %v %v", draft.UserID, draft.Published, draft.Views), "1 false 7")

	withParents := must(Users.Where(Users.ID.Eq(2)).Get(ctx))
	ok(orm.Load(ctx, withParents, UserPosts.Chaperone(func(p *Post, u *User) { p.Author = u })))
	eq(t, "chaperone", withParents[0].Posts[0].Author.Name, "Bob")

	post := must(Posts.Find(ctx, 4))
	eq(t, "belongsTo of", must(PostAuthor.Of(&post).First(ctx)).Name, "Bob")
	eq(t, "belongsTo whereHas", must(Posts.Query().WhereHas(PostAuthor, func(q orm.Query[User]) orm.Query[User] {
		return q.Where(Users.Name.Eq("Alice"))
	}).Count(ctx)), int64(4))

	eq(t, "wherePivot", must(UserRoles.WherePivot(RoleUsers.GrantedBy.Eq("system")).Of(&alice).Pluck(ctx, Roles.Name)), []string{"admin"})
	eq(t, "scoped belongsToMany", must(UserRoles.Scoped(func(q orm.Query[Role]) orm.Query[Role] {
		return q.Where(Roles.Name.Eq("editor"))
	}).Of(&alice).Pluck(ctx, Roles.Name)), []string{"editor"})

	firstPost := map[int64]string{}
	countryPost := orm.HasOneThrough(Countries.ID, Users.CountryID, Users.ID, Posts.UserID,
		func(c *Country, p *Post) {
			if p != nil {
				firstPost[c.ID] = p.Title
			}
		})
	countries := must(Countries.Query().OrderBy(Countries.ID.Asc()).Get(ctx))
	ok(orm.Load(ctx, countries, countryPost))
	eq(t, "hasOneThrough", firstPost[2], "Carol's post")
	eq(t, "hasOneThrough loads every parent", len(firstPost), 2)

	author := orm.BelongsTo(Posts.UserID, Users.ID, func(*Post, *User) {})
	author.Touches()
	before := must(Users.Find(ctx, 2)).UpdatedAt
	post.Title = "touched"
	ok(Posts.Save(ctx, &post))
	eq(t, "touches", must(Users.Find(ctx, 2)).UpdatedAt.After(before), true)
	touched := must(Users.Find(ctx, 2)).UpdatedAt
	post.Title = "quietly"
	ok(Posts.Save(orm.WithoutTimestamps(ctx), &post))
	eq(t, "no touch without timestamps", must(Users.Find(ctx, 2)).UpdatedAt.Equal(touched), true)
	eq(t, "saved without timestamps", must(Posts.Find(ctx, post.ID)).Title, "quietly")
}

type blockWrites struct{}

func TestQuietWritesAndFill(t *testing.T) {
	ctx := setup(t)
	Users.On(orm.Deleting, func(ctx context.Context, _ *User) error {
		if ctx.Value(blockWrites{}) != nil {
			return errors.New("blocked")
		}
		return nil
	})
	Users.On(orm.Restoring, func(ctx context.Context, _ *User) error {
		if ctx.Value(blockWrites{}) != nil {
			return errors.New("blocked")
		}
		return nil
	})
	blocked := context.WithValue(ctx, blockWrites{}, true)

	dave := must(Users.Find(ctx, 4))
	eq(t, "events block delete", Users.Delete(blocked, &dave) != nil, true)
	ok(Users.DeleteQuietly(blocked, &dave))
	eq(t, "deleteQuietly", must(Users.Query().OnlyTrashed().Count(ctx)), int64(2))
	ok(Users.RestoreQuietly(blocked, &dave))
	eq(t, "restoreQuietly", must(Users.Query().OnlyTrashed().Count(ctx)), int64(1))
	ok(Users.ForceDeleteQuietly(blocked, &dave))
	eq(t, "forceDeleteQuietly", must(Users.Query().WithTrashed().Count(ctx)), int64(4))

	eq(t, "query forceDelete", must(Users.Query().WithTrashed().Where(Users.Name.Eq("Eve")).ForceDelete(ctx)), int64(1))
	eq(t, "query forceDeleted", must(Users.Query().WithTrashed().Count(ctx)), int64(3))

	var u User
	orm.Fill(&u, Users.Name.Set("Filled"), Users.Karma.Set(3))
	eq(t, "fill", fmt.Sprintf("%v %v", u.Name, u.Karma), "Filled 3")
}

func TestFactoryOne(t *testing.T) {
	ctx := setup(t)
	users := orm.NewFactory(Users.Table, func(n int) User {
		return User{Name: fmt.Sprintf("One %d", n), Email: fmt.Sprintf("one%d@example.com", n), Active: true}
	})
	eq(t, "makeOne", users.MakeOne().Name, "One 1")
	created := must(users.CreateOne(ctx))
	eq(t, "createOne", must(Users.Find(ctx, created.ID)).Email, "one2@example.com")
}

func TestJSONCasts(t *testing.T) {
	settings := orm.JSON[Settings]{V: Settings{Theme: "dark"}}
	raw := must(json.Marshal(settings))
	var back orm.JSON[Settings]
	ok(json.Unmarshal(raw, &back))
	eq(t, "json cast round trip", back.V.Theme, "dark")
	eq(t, "json cast is transparent", strings.Contains(string(raw), `"theme":"dark"`) && !strings.Contains(string(raw), `"V"`), true)

	secret := orm.Encrypted[string]{V: "s3cret"}
	raw = must(json.Marshal(secret))
	eq(t, "encrypted marshals its plain value", string(raw), `"s3cret"`)
	var plain orm.Encrypted[string]
	ok(json.Unmarshal(raw, &plain))
	eq(t, "encrypted round trip", plain.V, "s3cret")

	vec := orm.NewVector3(1, 2.5, 3)
	raw = must(json.Marshal(vec))
	eq(t, "vector marshal", string(raw), "[1,2.5,3]")
	var vback orm.Vector[orm.D3]
	ok(json.Unmarshal(raw, &vback))
	eq(t, "vector round trip", vback.String(), vec.String())
	eq(t, "vector of the wrong size", json.Unmarshal([]byte("[1,2]"), &vback) != nil, true)
}

// panics reports whether f panics.
func panics(f func()) (did bool) {
	defer func() { did = recover() != nil }()
	f()
	return false
}

func TestScanners(t *testing.T) {
	var v orm.Vector[orm.D3]
	ok(v.Scan("[1, 2, 3]"))
	eq(t, "vector from text", v.String(), "[1,2,3]")
	ok(v.Scan([]byte("[4,5,6]")))
	eq(t, "vector from bytes", v.String(), "[4,5,6]")
	ok(v.Scan([]float32{1, 2, 3}))
	eq(t, "vector from float32s", v.String(), "[1,2,3]")
	ok(v.Scan([]float64{7, 8, 9}))
	eq(t, "vector from float64s", v.String(), "[7,8,9]")
	ok(v.Scan([]any{json.Number("1.5"), 2, int64(3)}))
	eq(t, "vector from decoded values", v.String(), "[1.5,2,3]")
	ok(v.Scan([]any{float32(1), "2", 3.0}))
	eq(t, "vector from mixed values", v.String(), "[1,2,3]")
	ok(v.Scan(nil))
	eq(t, "vector from null", v.Floats() == nil, true)
	ok(v.Scan(""))
	eq(t, "vector from empty text", v.Floats() == nil, true)
	eq(t, "vector of the wrong size from text", v.Scan("[1,2]") != nil, true)
	eq(t, "vector of the wrong size from floats", v.Scan([]float64{1}) != nil, true)
	eq(t, "vector of non-numbers", v.Scan("[a,b,c]") != nil, true)
	eq(t, "vector with a non-number element", v.Scan([]any{1, true, 3}) != nil, true)
	eq(t, "vector from an unsupported type", v.Scan(42) != nil, true)
	_, err := orm.VectorOf[orm.D3]([]float64{1, 2})
	eq(t, "vectorOf checks the dimension", err != nil, true)
	eq(t, "newVector panics on the wrong dimension", panics(func() { orm.NewVector[orm.D4](1, 2, 3) }), true)
	ok(json.Unmarshal([]byte("null"), &v))
	eq(t, "vector from json null", must(json.Marshal(v)), []byte("null"))

	var j orm.JSON[map[string]int]
	ok(j.Scan(map[string]any{"a": 1}))
	eq(t, "json from a decoded value", j.V["a"], 1)
	ok(j.Scan(nil))
	eq(t, "json from null", j.V == nil, true)
	eq(t, "json from an unencodable value", j.Scan(make(chan int)) != nil, true)

	orm.SetEncryptionKey([]byte("scanner test key"))
	sealedText := must(orm.Encrypted[string]{V: "s3cret"}.Value()).(string)
	var e orm.Encrypted[string]
	ok(e.Scan(sealedText))
	eq(t, "encrypted from text", e.V, "s3cret")
	ok(e.Scan([]byte(sealedText)))
	eq(t, "encrypted from bytes", e.V, "s3cret")
	for _, empty := range []any{nil, "", []byte("")} {
		e.V = "stale"
		ok(e.Scan(empty))
		eq(t, fmt.Sprintf("encrypted from %#v", empty), e.V, "")
	}
	eq(t, "encrypted from tampered text", e.Scan(sealedText[:len(sealedText)-4]+"AAAA") != nil, true)
	eq(t, "encrypted from non-base64", e.Scan("not base64!") != nil, true)
	eq(t, "encrypted from too little", e.Scan("AAAA") != nil, true)
	eq(t, "encrypted from an unsupported type", e.Scan(42) != nil, true)

	var h orm.Hashed
	hash := must(orm.Hashed("hunter2").Value()).(string)
	ok(h.Scan(hash))
	eq(t, "hashed from text", h.Matches("hunter2"), true)
	ok(h.Scan([]byte(hash)))
	eq(t, "hashed from bytes", h.Matches("wrong"), false)
	eq(t, "hashed keeps an existing hash", must(h.Value()), any(hash))
	ok(h.Scan(nil))
	eq(t, "hashed from null", h, orm.Hashed(""))
	eq(t, "hashed from an unsupported type", h.Scan(42) != nil, true)
}

func TestVectorDimensions(t *testing.T) {
	checkDim[orm.D2](t, 2)
	checkDim[orm.D3](t, 3)
	checkDim[orm.D4](t, 4)
	checkDim[orm.D5](t, 5)
	checkDim[orm.D6](t, 6)
	checkDim[orm.D7](t, 7)
	checkDim[orm.D8](t, 8)
	checkDim[orm.D9](t, 9)
	checkDim[orm.D10](t, 10)
	checkDim[orm.D12](t, 12)
	checkDim[orm.D16](t, 16)
	checkDim[orm.D24](t, 24)
	checkDim[orm.D32](t, 32)
	checkDim[orm.D48](t, 48)
	checkDim[orm.D64](t, 64)
	checkDim[orm.D96](t, 96)
	checkDim[orm.D128](t, 128)
	checkDim[orm.D160](t, 160)
	checkDim[orm.D256](t, 256)
	checkDim[orm.D384](t, 384)
	checkDim[orm.D512](t, 512)
	checkDim[orm.D640](t, 640)
	checkDim[orm.D768](t, 768)
	checkDim[orm.D896](t, 896)
	checkDim[orm.D1024](t, 1024)
	checkDim[orm.D1280](t, 1280)
	checkDim[orm.D1536](t, 1536)
	checkDim[orm.D2048](t, 2048)
	checkDim[orm.D3072](t, 3072)
	checkDim[orm.D4096](t, 4096)
}

// checkDim asserts the marker N admits exactly n components.
func checkDim[N orm.Dimension](t *testing.T, n int) {
	t.Helper()
	v, err := orm.VectorOf[N](make([]float64, n))
	ok(err)
	eq(t, fmt.Sprintf("D%d holds %d components", n, n), len(v.Floats()), n)
	_, err = orm.VectorOf[N](make([]float64, n+1))
	eq(t, fmt.Sprintf("D%d refuses %d components", n, n+1), err != nil, true)
}

type lifecycle struct{ events *[]string }

func (l lifecycle) record(ctx context.Context, e string) error {
	if ctx.Value(observing{}) == nil {
		return nil
	}
	*l.events = append(*l.events, e)
	if ctx.Value(abortOn{}) == e {
		return errors.New("aborted on " + e)
	}
	return nil
}

type (
	observing struct{}
	abortOn   struct{}
)

func (l lifecycle) Retrieved(ctx context.Context, _ *User) error { return l.record(ctx, "retrieved") }

func (l lifecycle) Creating(ctx context.Context, _ *User) error { return l.record(ctx, "creating") }

func (l lifecycle) Created(ctx context.Context, _ *User) error { return l.record(ctx, "created") }

func (l lifecycle) Updating(ctx context.Context, _ *User) error { return l.record(ctx, "updating") }

func (l lifecycle) Updated(ctx context.Context, _ *User) error { return l.record(ctx, "updated") }

func (l lifecycle) Saving(ctx context.Context, _ *User) error { return l.record(ctx, "saving") }

func (l lifecycle) Saved(ctx context.Context, _ *User) error { return l.record(ctx, "saved") }

func (l lifecycle) Deleting(ctx context.Context, _ *User) error { return l.record(ctx, "deleting") }

func (l lifecycle) Deleted(ctx context.Context, _ *User) error { return l.record(ctx, "deleted") }

func (l lifecycle) Trashed(ctx context.Context, _ *User) error { return l.record(ctx, "trashed") }

func (l lifecycle) ForceDeleting(ctx context.Context, _ *User) error {
	return l.record(ctx, "forceDeleting")
}

func (l lifecycle) ForceDeleted(ctx context.Context, _ *User) error {
	return l.record(ctx, "forceDeleted")
}

func (l lifecycle) Restoring(ctx context.Context, _ *User) error { return l.record(ctx, "restoring") }

func (l lifecycle) Restored(ctx context.Context, _ *User) error { return l.record(ctx, "restored") }

func (l lifecycle) Replicating(ctx context.Context, _ *User) error {
	return l.record(ctx, "replicating")
}

func TestObserverLifecycle(t *testing.T) {
	ctx := setup(t)
	var events []string
	Users.Observe(lifecycle{&events})
	watched := context.WithValue(ctx, observing{}, true)
	step := func(what string, f func() error, want ...string) {
		t.Helper()
		events = nil
		ok(f())
		eq(t, what, events, want)
	}

	u := User{Name: "Obs", Email: "obs@example.com", Active: true}
	step("create", func() error { return Users.Create(watched, &u) }, "saving", "creating", "created", "saved")
	u.Karma = 5
	step("update", func() error { return Users.Save(watched, &u) }, "saving", "updating", "updated", "saved")
	step("find", func() error { _, err := Users.Find(watched, u.ID); return err }, "retrieved")
	step("soft delete", func() error { return Users.Delete(watched, &u) }, "deleting", "trashed", "deleted")
	step("restore", func() error { return Users.Restore(watched, &u) }, "restoring", "saving", "updating", "updated", "saved", "restored")
	step("replicate", func() error { _, err := Users.Replicate(watched, &u); return err }, "replicating")
	step("force delete", func() error { return Users.ForceDelete(watched, &u) }, "forceDeleting", "deleting", "deleted", "forceDeleted")

	before := must(Users.Query().WithTrashed().Count(ctx))
	aborted := context.WithValue(watched, abortOn{}, "creating")
	err := Users.Create(aborted, &User{Name: "Never", Email: "never@example.com"})
	eq(t, "creating aborts", err != nil && strings.Contains(err.Error(), "aborted on creating"), true)
	eq(t, "nothing inserted", must(Users.Query().WithTrashed().Count(ctx)), before)
	eq(t, "an observer without methods panics", panics(func() { Users.Observe(struct{}{}) }), true)
}

func TestMaintainedTimePrecision(t *testing.T) {
	ctx := setup(t)
	post := must(Posts.Find(ctx, 1))
	c := Comment{Body: "precise"}
	ok(PostComments.Create(ctx, &post, &c))
	eq(t, "microsecond timestamps", c.CreatedAt.Nanosecond()%1000 == 0 && c.CreatedAt.Equal(c.UpdatedAt), true)
	eq(t, "they round-trip", must(Comments.Find(ctx, c.ID)).CreatedAt.Equal(c.CreatedAt), true)
	fresh := Post{UserID: 1, Title: "whole seconds"}
	ok(Posts.Create(ctx, &fresh))
	eq(t, "default precision is whole seconds", fresh.CreatedAt.Nanosecond(), 0)
}
