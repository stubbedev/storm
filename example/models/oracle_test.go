package models_test

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
)

const oracleRounds = 150

func oracleRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := uint64(20261011)
	if s := os.Getenv("STORM_ORACLE_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("STORM_ORACLE_SEED %q: %v", s, err)
		}
		seed = v
	}
	t.Logf("seed %d (replay with STORM_ORACLE_SEED=%d)", seed, seed)
	return rand.New(rand.NewPCG(seed, seed))
}

type oUser struct {
	id      int64
	country int64
	name    string
	karma   int
	active  bool
	trashed bool
}

type oPost struct {
	id        int64
	user      int64
	title     string
	published bool
	views     int64
}

type world struct {
	countries []string
	roles     []string
	users     []oUser
	posts     []oPost
	grants    map[int64][]int64
}

var (
	nameParts  = []string{"ann", "Ann", "bo", "b_o", "c%d", "d*", "e?f", "[g]", `h\i`, "jo jo", "KAI", "ka"}
	titleWords = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}
)

func pick[T any](rng *rand.Rand, xs []T) T { return xs[rng.IntN(len(xs))] }

func seedWorld(t *testing.T, rng *rand.Rand) world {
	t.Helper()
	for _, table := range []string{"role_user", "taggables", "images", "comments", "posts", "users", "countries", "roles", "tags", "videos"} {
		must(orm.From(table).Delete())
	}
	w := world{
		countries: []string{"denmark", "norway", "sweden", "finland"},
		roles:     []string{"admin", "editor", "viewer"},
		grants:    map[int64][]int64{},
	}
	var countries, roles, users, posts, grants []map[string]any
	for i, name := range w.countries {
		countries = append(countries, map[string]any{"id": i + 1, "name": name})
	}
	for i, name := range w.roles {
		roles = append(roles, map[string]any{"id": i + 1, "name": name})
	}
	for id := int64(1); id <= 60; id++ {
		u := oUser{
			id:      id,
			country: int64(rng.IntN(len(w.countries) + 1)),
			name:    pick(rng, nameParts),
			karma:   rng.IntN(21),
			active:  rng.IntN(3) > 0,
			trashed: rng.IntN(5) == 0,
		}
		if rng.IntN(2) == 0 {
			u.name += pick(rng, nameParts)
		}
		w.users = append(w.users, u)
		row := map[string]any{"id": u.id, "name": u.name, "email": fmt.Sprintf("u%d@example.com", u.id), "active": u.active, "karma": u.karma}
		if u.country != 0 {
			row["country_id"] = u.country
		}
		if u.trashed {
			row["deleted_at"] = "2026-01-01 00:00:00"
		}
		users = append(users, row)
		for r := range len(w.roles) {
			if rng.IntN(2) == 0 {
				w.grants[id] = append(w.grants[id], int64(r+1))
				grants = append(grants, map[string]any{"user_id": id, "role_id": r + 1, "granted_by": fmt.Sprint("by", id)})
			}
		}
	}
	owners := make([]int64, 0, 70)
	for _, u := range w.users {
		for range rng.IntN(5) {
			owners = append(owners, u.id)
		}
	}
	owners = append(owners, 1001, 1002, 1003)
	for i, owner := range owners {
		p := oPost{id: int64(i + 1), user: owner, title: pick(rng, titleWords), published: rng.IntN(2) == 0, views: int64(rng.IntN(31))}
		w.posts = append(w.posts, p)
		posts = append(posts, map[string]any{"id": p.id, "user_id": p.user, "title": p.title, "published": p.published, "views": p.views})
	}
	ok(orm.From("countries").Insert(countries...))
	ok(orm.From("roles").Insert(roles...))
	ok(orm.From("users").Insert(users...))
	ok(orm.From("posts").Insert(posts...))
	ok(orm.From("role_user").Insert(grants...))
	return w
}

func (w world) user(id int64) (oUser, bool) {
	i := slices.IndexFunc(w.users, func(u oUser) bool { return u.id == id })
	if i < 0 {
		return oUser{}, false
	}
	return w.users[i], true
}

func (w world) postsOf(id int64, keep func(oPost) bool) []oPost {
	var out []oPost
	for _, p := range w.posts {
		if p.user == id && keep(p) {
			out = append(out, p)
		}
	}
	return out
}

type tri int8

const (
	no tri = iota
	unknown
	yes
)

func truth(b bool) tri {
	if b {
		return yes
	}
	return no
}

func and3(ts ...tri) tri {
	out := yes
	for _, t := range ts {
		out = min(out, t)
	}
	return out
}

func or3(ts ...tri) tri {
	out := no
	for _, t := range ts {
		out = max(out, t)
	}
	return out
}

func not3(t tri) tri { return yes - t }

type userPred struct {
	cond orm.Cond[User]
	eval func(oUser) tri
	desc string
}

type postPred struct {
	scope func(PostQuery) PostQuery
	eval  func(oPost) bool
	desc  string
}

func randomPostPred(rng *rand.Rand) postPred {
	switch rng.IntN(4) {
	case 0:
		return postPred{func(q PostQuery) PostQuery { return q }, func(oPost) bool { return true }, "any post"}
	case 1:
		b := rng.IntN(2) == 0
		return postPred{func(q PostQuery) PostQuery { return q.Where(Posts.Published.Eq(b)) }, func(p oPost) bool { return p.published == b }, fmt.Sprint("published=", b)}
	case 2:
		v := int64(rng.IntN(31))
		return postPred{func(q PostQuery) PostQuery { return q.Where(Posts.Views.Gte(v)) }, func(p oPost) bool { return p.views >= v }, fmt.Sprint("views>=", v)}
	default:
		v, b := int64(rng.IntN(31)), rng.IntN(2) == 0
		return postPred{
			func(q PostQuery) PostQuery { return q.Where(orm.Or(Posts.Views.Lt(v), Posts.Published.Eq(b))) },
			func(p oPost) bool { return p.views < v || p.published == b },
			fmt.Sprintf("views<%d or published=%v", v, b),
		}
	}
}

func randomInts(rng *rand.Rand, n, below int) []int {
	out := make([]int, rng.IntN(n+1))
	for i := range out {
		out[i] = rng.IntN(below)
	}
	return out
}

func toInt64s(xs []int) []int64 {
	out := make([]int64, len(xs))
	for i, x := range xs {
		out[i] = int64(x)
	}
	return out
}

func randomLike(rng *rand.Rand, fold bool) string {
	name := pick(rng, nameParts) + pick(rng, nameParts)
	from := rng.IntN(len(name))
	to := from + 1 + rng.IntN(len(name)-from)
	var b strings.Builder
	for _, r := range name[from:to] {
		switch {
		case rng.IntN(5) == 0:
			b.WriteByte('_')
		case fold && rng.IntN(2) == 0:
			b.WriteString(orm.EscapeLike(strings.ToUpper(string(r))))
		default:
			b.WriteString(orm.EscapeLike(string(r)))
		}
	}
	return pick(rng, []string{"%", ""}) + b.String() + pick(rng, []string{"%", ""})
}

func likeMatcher(pattern string, fold bool) *regexp.Regexp {
	var b strings.Builder
	b.WriteString(map[bool]string{false: "(?s)^", true: "(?is)^"}[fold])
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			escaped = false
			b.WriteString(regexp.QuoteMeta(string(r)))
		case r == '\\':
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String())
}

func randomAtom(rng *rand.Rand, w world) userPred {
	k := rng.IntN(21)
	c := int64(1 + rng.IntN(len(w.countries)))
	country := func(u oUser, f func(int64) bool) tri {
		if u.country == 0 {
			return unknown
		}
		return truth(f(u.country))
	}
	switch rng.IntN(22) {
	case 0:
		return userPred{Users.Karma.Gt(k), func(u oUser) tri { return truth(u.karma > k) }, fmt.Sprint("karma>", k)}
	case 1:
		return userPred{Users.Karma.Gte(k), func(u oUser) tri { return truth(u.karma >= k) }, fmt.Sprint("karma>=", k)}
	case 2:
		return userPred{Users.Karma.Lt(k), func(u oUser) tri { return truth(u.karma < k) }, fmt.Sprint("karma<", k)}
	case 3:
		return userPred{Users.Karma.Lte(k), func(u oUser) tri { return truth(u.karma <= k) }, fmt.Sprint("karma<=", k)}
	case 4:
		return userPred{Users.Karma.Eq(k), func(u oUser) tri { return truth(u.karma == k) }, fmt.Sprint("karma=", k)}
	case 5:
		return userPred{Users.Karma.Ne(k), func(u oUser) tri { return truth(u.karma != k) }, fmt.Sprint("karma!=", k)}
	case 6:
		lo, hi := rng.IntN(21), rng.IntN(21)
		return userPred{Users.Karma.Between(lo, hi), func(u oUser) tri { return truth(lo <= u.karma && u.karma <= hi) }, fmt.Sprintf("karma between %d and %d", lo, hi)}
	case 7:
		lo, hi := rng.IntN(21), rng.IntN(21)
		return userPred{Users.Karma.NotBetween(lo, hi), func(u oUser) tri { return truth(u.karma < lo || u.karma > hi) }, fmt.Sprintf("karma not between %d and %d", lo, hi)}
	case 8:
		ks := randomInts(rng, 3, 21)
		return userPred{Users.Karma.In(ks...), func(u oUser) tri { return truth(slices.Contains(ks, u.karma)) }, fmt.Sprint("karma in ", ks)}
	case 9:
		ks := randomInts(rng, 3, 21)
		return userPred{Users.Karma.NotIn(ks...), func(u oUser) tri { return truth(!slices.Contains(ks, u.karma)) }, fmt.Sprint("karma not in ", ks)}
	case 10:
		b := rng.IntN(2) == 0
		return userPred{Users.Active.Eq(b), func(u oUser) tri { return truth(u.active == b) }, fmt.Sprint("active=", b)}
	case 11:
		return userPred{Users.CountryID.Eq(c), func(u oUser) tri { return country(u, func(x int64) bool { return x == c }) }, fmt.Sprint("country=", c)}
	case 12:
		return userPred{Users.CountryID.Ne(c), func(u oUser) tri { return country(u, func(x int64) bool { return x != c }) }, fmt.Sprint("country!=", c)}
	case 13:
		cs := toInt64s(randomInts(rng, 3, len(w.countries)+1))
		cs = slices.DeleteFunc(cs, func(x int64) bool { return x == 0 })
		if len(cs) == 0 {
			return userPred{Users.CountryID.NotIn(), func(oUser) tri { return yes }, "country not in ()"}
		}
		if rng.IntN(2) == 0 {
			return userPred{Users.CountryID.In(cs...), func(u oUser) tri { return country(u, func(x int64) bool { return slices.Contains(cs, x) }) }, fmt.Sprint("country in ", cs)}
		}
		return userPred{Users.CountryID.NotIn(cs...), func(u oUser) tri { return country(u, func(x int64) bool { return !slices.Contains(cs, x) }) }, fmt.Sprint("country not in ", cs)}
	case 14:
		if rng.IntN(2) == 0 {
			return userPred{Users.CountryID.IsNull(), func(u oUser) tri { return truth(u.country == 0) }, "country is null"}
		}
		return userPred{Users.CountryID.IsNotNull(), func(u oUser) tri { return truth(u.country != 0) }, "country is not null"}
	case 15:
		fold := rng.IntN(2) == 0
		pattern := randomLike(rng, fold)
		re := likeMatcher(pattern, fold)
		match := func(u oUser) tri { return truth(re.MatchString(u.name)) }
		negate := rng.IntN(3) == 0
		switch {
		case fold && negate:
			return userPred{Users.Name.NotILike(pattern), func(u oUser) tri { return not3(match(u)) }, "name not ilike " + pattern}
		case fold:
			return userPred{Users.Name.ILike(pattern), match, "name ilike " + pattern}
		case negate:
			return userPred{Users.Name.NotLike(pattern), func(u oUser) tri { return not3(match(u)) }, "name not like " + pattern}
		default:
			return userPred{Users.Name.Like(pattern), match, "name like " + pattern}
		}
	case 16:
		part := pick(rng, nameParts)
		part = part[rng.IntN(len(part)):]
		switch rng.IntN(3) {
		case 0:
			return userPred{Users.Name.Contains(part), func(u oUser) tri { return truth(strings.Contains(u.name, part)) }, "name contains " + part}
		case 1:
			return userPred{Users.Name.StartsWith(part), func(u oUser) tri { return truth(strings.HasPrefix(u.name, part)) }, "name starts with " + part}
		default:
			return userPred{Users.Name.EndsWith(part), func(u oUser) tri { return truth(strings.HasSuffix(u.name, part)) }, "name ends with " + part}
		}
	case 17, 18:
		p := randomPostPred(rng)
		has := func(u oUser) bool { return len(w.postsOf(u.id, p.eval)) > 0 }
		if rng.IntN(2) == 0 {
			return userPred{orm.Has(UserPosts, p.scope), func(u oUser) tri { return truth(has(u)) }, "has posts with " + p.desc}
		}
		return userPred{orm.DoesntHave(UserPosts, p.scope), func(u oUser) tri { return truth(!has(u)) }, "has no posts with " + p.desc}
	case 19:
		roles := toInt64s(randomInts(rng, 2, len(w.roles)))
		for i := range roles {
			roles[i]++
		}
		return userPred{
			orm.Has(UserRoles, func(q RoleQuery) RoleQuery { return q.Where(Roles.ID.In(roles...)) }),
			func(u oUser) tri {
				return truth(slices.ContainsFunc(w.grants[u.id], func(r int64) bool { return slices.Contains(roles, r) }))
			},
			fmt.Sprint("has a role in ", roles),
		}
	case 20:
		name := pick(rng, w.countries)
		return userPred{
			orm.Has(UserCountry, func(q CountryQuery) CountryQuery { return q.Where(Countries.Name.Eq(name)) }),
			func(u oUser) tri { return truth(u.country != 0 && w.countries[u.country-1] == name) },
			"has country " + name,
		}
	default:
		p := randomPostPred(rng)
		return userPred{
			orm.Has(UserLatestPost, p.scope),
			func(u oUser) tri {
				all := w.postsOf(u.id, func(oPost) bool { return true })
				return truth(len(all) > 0 && p.eval(all[len(all)-1]))
			},
			"latest post has " + p.desc,
		}
	}
}

func randomPred(rng *rand.Rand, w world, depth int) userPred {
	if depth == 0 || rng.IntN(3) == 0 {
		return randomAtom(rng, w)
	}
	parts := make([]userPred, rng.IntN(4))
	for i := range parts {
		parts[i] = randomPred(rng, w, depth-1)
	}
	conds := make([]orm.Cond[User], len(parts))
	descs := make([]string, len(parts))
	for i, p := range parts {
		conds[i], descs[i] = p.cond, p.desc
	}
	evalAll := func(u oUser) []tri {
		out := make([]tri, len(parts))
		for i, p := range parts {
			out[i] = p.eval(u)
		}
		return out
	}
	switch rng.IntN(3) {
	case 0:
		return userPred{orm.And(conds...), func(u oUser) tri { return and3(evalAll(u)...) }, "(" + strings.Join(descs, " and ") + ")"}
	case 1:
		return userPred{orm.Or(conds...), func(u oUser) tri { return or3(evalAll(u)...) }, "(" + strings.Join(descs, " or ") + ")"}
	default:
		inner := randomPred(rng, w, depth-1)
		return userPred{orm.Not(inner.cond), func(u oUser) tri { return not3(inner.eval(u)) }, "not " + inner.desc}
	}
}

type trashedMode int

const (
	withoutTrashed trashedMode = iota
	withTrashed
	onlyTrashed
)

func (m trashedMode) String() string {
	return [...]string{withoutTrashed: "without trashed", withTrashed: "with trashed", onlyTrashed: "only trashed"}[m]
}

func (m trashedMode) keeps(u oUser) bool {
	return m == withTrashed || (m == onlyTrashed) == u.trashed
}

func (m trashedMode) apply(q UserQuery) UserQuery {
	switch m {
	case withTrashed:
		return q.WithTrashed()
	case onlyTrashed:
		return q.OnlyTrashed()
	}
	return q
}

type userFilter struct {
	apply func(UserQuery) UserQuery
	keeps func(oUser) bool
	desc  string
}

func randomFilter(rng *rand.Rand, w world, mode trashedMode) userFilter {
	type step struct {
		apply func(UserQuery) UserQuery
		fold  func(cur tri, seen bool, u oUser) tri
		desc  string
	}
	var steps []step
	for range rng.IntN(4) {
		preds := make([]userPred, 1+rng.IntN(2))
		conds := make([]orm.Cond[User], len(preds))
		descs := make([]string, len(preds))
		for i := range preds {
			preds[i] = randomPred(rng, w, 2)
			conds[i], descs[i] = preds[i].cond, preds[i].desc
		}
		all := func(u oUser) tri {
			out := yes
			for _, p := range preds {
				out = and3(out, p.eval(u))
			}
			return out
		}
		or, negate := rng.IntN(3) == 0, rng.IntN(3) == 0
		calls := map[[2]bool]struct {
			call func(UserQuery, ...orm.Cond[User]) UserQuery
			name string
		}{
			{false, false}: {UserQuery.Where, "where"},
			{true, false}:  {UserQuery.OrWhere, "or where"},
			{false, true}:  {UserQuery.WhereNot, "where not"},
			{true, true}:   {UserQuery.OrWhereNot, "or where not"},
		}[[2]bool{or, negate}]
		steps = append(steps, step{
			apply: func(q UserQuery) UserQuery { return calls.call(q, conds...) },
			fold: func(cur tri, seen bool, u oUser) tri {
				v := all(u)
				if negate {
					v = not3(v)
				}
				if or && seen {
					return or3(cur, v)
				}
				return and3(cur, v)
			},
			desc: calls.name + " " + strings.Join(descs, ", "),
		})
	}
	if rng.IntN(4) == 0 {
		p := randomPostPred(rng)
		op, n := pick(rng, []orm.Op{orm.Eq, orm.Neq, orm.Gt, orm.Gte, orm.Lt, orm.Lte}), int64(rng.IntN(4))
		holds := map[orm.Op]func(a, b int64) bool{
			orm.Eq: func(a, b int64) bool { return a == b }, orm.Neq: func(a, b int64) bool { return a != b },
			orm.Gt: func(a, b int64) bool { return a > b }, orm.Gte: func(a, b int64) bool { return a >= b },
			orm.Lt: func(a, b int64) bool { return a < b }, orm.Lte: func(a, b int64) bool { return a <= b },
		}[op]
		steps = append(steps, step{
			apply: func(q UserQuery) UserQuery { return q.HasCount(UserPosts, op, n, p.scope) },
			fold: func(cur tri, _ bool, u oUser) tri {
				return and3(cur, truth(holds(int64(len(w.postsOf(u.id, p.eval))), n)))
			},
			desc: fmt.Sprintf("has count(posts with %s) %s %d", p.desc, op, n),
		})
	}
	descs := []string{mode.String()}
	for _, s := range steps {
		descs = append(descs, s.desc)
	}
	return userFilter{
		apply: func(q UserQuery) UserQuery {
			q = mode.apply(q)
			for _, s := range steps {
				q = s.apply(q)
			}
			return q
		},
		keeps: func(u oUser) bool {
			if !mode.keeps(u) {
				return false
			}
			cur, seen := yes, false
			for _, s := range steps {
				cur, seen = s.fold(cur, seen, u), true
			}
			return cur == yes
		},
		desc: strings.Join(descs, "\n  "),
	}
}

type userOrder struct {
	orders  []orm.Order[User]
	compare func(a, b oUser) int
	desc    string
}

func randomOrder(rng *rand.Rand) userOrder {
	type term struct {
		asc, desc orm.Order[User]
		key       func(oUser) (int64, bool)
		name      string
	}
	boolKey := func(b bool) int64 {
		if b {
			return 1
		}
		return 0
	}
	terms := []term{
		{Users.Karma.Asc(), Users.Karma.Desc(), func(u oUser) (int64, bool) { return int64(u.karma), false }, "karma"},
		{Users.Active.Asc(), Users.Active.Desc(), func(u oUser) (int64, bool) { return boolKey(u.active), false }, "active"},
		{Users.CountryID.Asc().NullsLast(), Users.CountryID.Desc().NullsLast(), func(u oUser) (int64, bool) { return u.country, u.country == 0 }, "country_id nulls last"},
	}
	rng.Shuffle(len(terms), func(i, j int) { terms[i], terms[j] = terms[j], terms[i] })
	terms = terms[:rng.IntN(len(terms)+1)]
	terms = append(terms, term{Users.ID.Asc(), Users.ID.Desc(), func(u oUser) (int64, bool) { return u.id, false }, "id"})
	var out userOrder
	descending := make([]bool, len(terms))
	var names []string
	for i, t := range terms {
		descending[i] = rng.IntN(2) == 0
		out.orders = append(out.orders, map[bool]orm.Order[User]{false: t.asc, true: t.desc}[descending[i]])
		names = append(names, t.name+map[bool]string{false: " asc", true: " desc"}[descending[i]])
	}
	out.desc = strings.Join(names, ", ")
	out.compare = func(a, b oUser) int {
		for i, t := range terms {
			av, an := t.key(a)
			bv, bn := t.key(b)
			if an != bn {
				return map[bool]int{true: 1, false: -1}[an]
			}
			c := cmp.Compare(av, bv)
			if descending[i] {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return 0
	}
	return out
}

func userIDs(us []User) []int64 {
	out := make([]int64, len(us))
	for i, u := range us {
		out[i] = u.ID
	}
	return out
}

func expectedUsers(w world, f userFilter, o userOrder) []oUser {
	var out []oUser
	for _, u := range w.users {
		if f.keeps(u) {
			out = append(out, u)
		}
	}
	slices.SortStableFunc(out, o.compare)
	return out
}

func oIDs(us []oUser) []int64 {
	out := make([]int64, len(us))
	for i, u := range us {
		out[i] = u.id
	}
	return out
}

func check[T any](t *testing.T, what, about string, got, want T) {
	t.Helper()
	if !equalish(got, want) {
		t.Fatalf("%s:\n got  %v\n want %v\nquery:\n  %s", what, got, want, about)
	}
}

func TestOracleQueries(t *testing.T) {
	setup(t)
	rng := oracleRand(t)
	w := seedWorld(t, rng)
	for range oracleRounds {
		f := randomFilter(rng, w, trashedMode(rng.IntN(3)))
		o := randomOrder(rng)
		q := f.apply(Users.Query()).OrderBy(o.orders...)
		sql, _ := rendered(q.ToSQL())
		about := f.desc + "\n  order by " + o.desc + "\n  " + sql
		wantUsers := expectedUsers(w, f, o)
		want := oIDs(wantUsers)
		got := must(q.Get())
		check(t, "get", about, userIDs(got), want)
		check(t, "count", about, must(q.Count()), int64(len(want)))
		check(t, "exists", about, must(q.Exists()), len(want) > 0)
		check(t, "doesn't exist", about, must(q.DoesntExist()), len(want) == 0)
		check(t, "pluck", about, must(q.Pluck(Users.ID)), want)

		karma := 0
		for _, u := range wantUsers {
			karma += u.karma
		}
		check(t, "sum", about, must(q.Sum(Users.Karma)), karma)
		if len(wantUsers) > 0 {
			ks := make([]int, len(wantUsers))
			for i, u := range wantUsers {
				ks[i] = u.karma
			}
			check(t, "min", about, must(q.Min(Users.Karma)), slices.Min(ks))
			check(t, "max", about, must(q.Max(Users.Karma)), slices.Max(ks))
			if avg := must(q.Avg(Users.Karma)); math.Abs(avg-float64(karma)/float64(len(ks))) > 1e-9 {
				t.Fatalf("avg: got %v, want %v\nquery:\n  %s", avg, float64(karma)/float64(len(ks)), about)
			}
			first := must(q.First())
			check(t, "first", about, first.ID, want[0])
		} else {
			_, minErr := q.Min(Users.Karma)
			_, maxErr := q.Max(Users.Karma)
			_, avgErr := q.Avg(Users.Karma)
			_, firstErr := q.First()
			for _, c := range []struct {
				what string
				err  error
			}{{"min", minErr}, {"max", maxErr}, {"avg", avgErr}, {"first", firstErr}} {
				if !errors.Is(c.err, orm.ErrNotFound) {
					t.Fatalf("%s of no rows: got %v, want orm.ErrNotFound\nquery:\n  %s", c.what, c.err, about)
				}
			}
		}

		offset, limit := rng.IntN(len(want)+2), 1+rng.IntN(10)
		check(t, fmt.Sprintf("offset %d limit %d", offset, limit), about,
			userIDs(must(q.Offset(offset).Limit(limit).Get())), want[min(offset, len(want)):min(offset+limit, len(want))])

		perPage := 1 + rng.IntN(12)
		lastPage := max(1, (len(want)+perPage-1)/perPage)
		page := 1 + rng.IntN(lastPage+1)
		from, to := min((page-1)*perPage, len(want)), min(page*perPage, len(want))
		p := must(q.Paginate(perPage, page))
		check(t, fmt.Sprintf("paginate(%d, %d) items", perPage, page), about, userIDs(p.Items), want[from:to])
		check(t, "paginate total", about, p.Total, int64(len(want)))
		check(t, "paginate last page", about, p.LastPage, lastPage)
		check(t, "paginate current page", about, p.CurrentPage, page)
		check(t, "paginate has more", about, p.HasMore, page < lastPage)
		wantFrom, wantTo := int64(0), int64(0)
		if to > from {
			wantFrom, wantTo = int64(from+1), int64(to)
		}
		check(t, "paginate from", about, p.From, wantFrom)
		check(t, "paginate to", about, p.To, wantTo)
		sp := must(q.SimplePaginate(perPage, page))
		check(t, "simple paginate items", about, userIDs(sp.Items), want[from:to])
		check(t, "simple paginate has more", about, sp.HasMore, to < len(want))

		forward, backward := walkCursor(t, func(cursor string) orm.CursorPage[User] {
			return must(q.CursorPaginate(perPage, cursor))
		}, func(u User) string { return strconv.FormatInt(u.ID, 10) })
		wantKeys := make([]string, len(want))
		for i, id := range want {
			wantKeys[i] = strconv.FormatInt(id, 10)
		}
		check(t, fmt.Sprintf("cursor pages of %d forward", perPage), about, forward, wantKeys)
		check(t, fmt.Sprintf("cursor pages of %d backward", perPage), about, backward, wantKeys)

		var chunked []int64
		ok(q.Chunk(perPage, func(us []User) error {
			if len(us) == 0 || len(us) > perPage {
				t.Fatalf("chunk of %d holds %d models\nquery:\n  %s", perPage, len(us), about)
			}
			chunked = append(chunked, userIDs(us)...)
			return nil
		}))
		check(t, "chunk", about, chunked, want)

		byID := slices.Sorted(slices.Values(want))
		var chunkedByID, lazy []int64
		ok(f.apply(Users.Query()).ChunkByID(perPage, func(us []User) error {
			chunkedByID = append(chunkedByID, userIDs(us)...)
			return nil
		}))
		check(t, "chunk by id", about, chunkedByID, byID)
		for u, err := range f.apply(Users.Query()).Lazy(perPage) {
			ok(err)
			lazy = append(lazy, u.ID)
		}
		check(t, "lazy", about, lazy, byID)
		var cursor []int64
		for u, err := range q.Cursor() {
			ok(err)
			cursor = append(cursor, u.ID)
		}
		check(t, "cursor", about, cursor, want)
	}
}

func TestOracleRelations(t *testing.T) {
	setup(t)
	rng := oracleRand(t)
	w := seedWorld(t, rng)
	byViews := func(a, b oPost) int { return cmp.Or(-cmp.Compare(a.views, b.views), -cmp.Compare(a.id, b.id)) }
	for range oracleRounds / 3 {
		f := randomFilter(rng, w, trashedMode(rng.IntN(3)))
		p := randomPostPred(rng)
		limit := rng.IntN(4)
		roleLimit := rng.IntN(3)
		posts := func(q PostQuery) PostQuery {
			q = p.scope(q).OrderBy(Posts.Views.Desc(), Posts.ID.Desc())
			if limit > 0 {
				q = q.Limit(limit)
			}
			return q
		}
		roles := func(q RoleQuery) RoleQuery {
			q = q.OrderBy(Roles.ID.Asc())
			if roleLimit > 0 {
				q = q.Limit(roleLimit)
			}
			return q
		}
		q := f.apply(Users.Query()).
			OrderBy(Users.ID.Asc()).
			With(UserPosts, posts).
			With(UserRoles, roles).
			With(UserLatestPost).
			With(UserCountry).
			WithCount(UserPosts, Users.PostsCount, p.scope).
			WithSum(UserPosts, Posts.Views, Users.PostViews, p.scope).
			WithExists(UserPosts, Users.HasAvatar, p.scope).
			WithAvg(UserPosts, Posts.Views, Users.AvgPostViews, p.scope).
			WithMax(UserPosts, Posts.Title, Users.LatestPostTitle, p.scope)
		sql, _ := rendered(q.ToSQL())
		about := fmt.Sprintf("%s\n  posts with %s, limit %d, roles limit %d\n  %s", f.desc, p.desc, limit, roleLimit, sql)
		got := must(q.Get())
		want := expectedUsers(w, f, userOrder{compare: func(a, b oUser) int { return cmp.Compare(a.id, b.id) }})
		check(t, "users", about, userIDs(got), oIDs(want))

		plain := must(f.apply(Users.Query()).OrderBy(Users.ID.Asc()).Get())
		bg := t.Context()
		ok(orm.Load(bg, plain, UserPosts, posts))
		ok(orm.LoadCount(bg, plain, UserPosts, Users.PostsCount, p.scope))
		ok(orm.LoadSum(bg, plain, UserPosts, Posts.Views, Users.PostViews, p.scope))
		ok(orm.LoadExists(bg, plain, UserPosts, Users.HasAvatar, p.scope))

		for i, u := range got {
			o := want[i]
			matching := w.postsOf(o.id, p.eval)
			slices.SortFunc(matching, byViews)
			eager := matching
			if limit > 0 {
				eager = matching[:min(limit, len(matching))]
			}
			wantPosts := make([]int64, len(eager))
			for j, p := range eager {
				wantPosts[j] = p.id
			}
			gotPosts := make([]int64, len(u.Posts))
			for j, p := range u.Posts {
				gotPosts[j] = p.ID
			}
			who := fmt.Sprintf("user %d: ", o.id)
			check(t, who+"eager posts", about, gotPosts, wantPosts)
			loaded := make([]int64, len(plain[i].Posts))
			for j, p := range plain[i].Posts {
				loaded[j] = p.ID
			}
			check(t, who+"loaded posts", about, loaded, wantPosts)

			var views int64
			var top string
			for _, p := range matching {
				views += p.views
				top = max(top, p.title)
			}
			check(t, who+"with count", about, u.PostsCount, int64(len(matching)))
			check(t, who+"with sum", about, u.PostViews, views)
			check(t, who+"with exists", about, u.HasAvatar, len(matching) > 0)
			check(t, who+"with max", about, u.LatestPostTitle, top)
			wantAvg := 0.0
			if len(matching) > 0 {
				wantAvg = float64(views) / float64(len(matching))
			}
			if math.Abs(u.AvgPostViews-wantAvg) > 1e-9 {
				t.Fatalf("%swith avg: got %v, want %v\nquery:\n  %s", who, u.AvgPostViews, wantAvg, about)
			}
			check(t, who+"load count", about, plain[i].PostsCount, int64(len(matching)))
			check(t, who+"load sum", about, plain[i].PostViews, views)
			check(t, who+"load exists", about, plain[i].HasAvatar, len(matching) > 0)

			grants := w.grants[o.id]
			if roleLimit > 0 {
				grants = grants[:min(roleLimit, len(grants))]
			}
			gotRoles := make([]int64, len(u.Roles))
			for j, r := range u.Roles {
				gotRoles[j] = r.ID
				check(t, who+"pivot user", about, r.Pivot.UserID, o.id)
				check(t, who+"pivot role", about, r.Pivot.RoleID, r.ID)
				check(t, who+"pivot granted by", about, r.Pivot.GrantedBy, fmt.Sprint("by", o.id))
			}
			check(t, who+"roles", about, gotRoles, grants)

			all := w.postsOf(o.id, func(oPost) bool { return true })
			switch {
			case len(all) == 0 && u.LatestPost != nil:
				t.Fatalf("%slatest post: got %d, want none\nquery:\n  %s", who, u.LatestPost.ID, about)
			case len(all) > 0 && (u.LatestPost == nil || u.LatestPost.ID != all[len(all)-1].id):
				t.Fatalf("%slatest post: got %v, want %d\nquery:\n  %s", who, u.LatestPost, all[len(all)-1].id, about)
			}
			switch {
			case o.country == 0 && u.Country != nil:
				t.Fatalf("%scountry: got %d, want none\nquery:\n  %s", who, u.Country.ID, about)
			case o.country != 0 && (u.Country == nil || u.Country.Name != w.countries[o.country-1]):
				t.Fatalf("%scountry: got %v, want %s\nquery:\n  %s", who, u.Country, w.countries[o.country-1], about)
			}
		}
	}

	authors := must(Posts.Query().OrderBy(Posts.ID.Asc()).With(PostAuthor).Get())
	check(t, "posts", "all posts with their authors", len(authors), len(w.posts))
	for i, p := range authors {
		want := "Guest Author"
		if u, found := w.user(w.posts[i].user); found && !u.trashed {
			want = u.name
		}
		check(t, fmt.Sprintf("post %d author", p.ID), "all posts with their authors", p.Author.Name, want)
	}

	for range oracleRounds / 3 {
		p := randomPostPred(rng)
		limit := rng.IntN(4)
		about := fmt.Sprintf("countries with posts with %s, limit %d", p.desc, limit)
		countries := must(Countries.Query().OrderBy(Countries.ID.Asc()).With(CountryPosts, func(q PostQuery) PostQuery {
			q = p.scope(q).OrderBy(Posts.Views.Desc(), Posts.ID.Desc())
			if limit > 0 {
				q = q.Limit(limit)
			}
			return q
		}).Get())
		check(t, "countries", about, len(countries), len(w.countries))
		for _, c := range countries {
			var want []oPost
			for _, post := range w.posts {
				u, found := w.user(post.user)
				if found && !u.trashed && u.country == c.ID && p.eval(post) {
					want = append(want, post)
				}
			}
			slices.SortFunc(want, byViews)
			if limit > 0 {
				want = want[:min(limit, len(want))]
			}
			wantIDs := make([]int64, len(want))
			for i, p := range want {
				wantIDs[i] = p.id
			}
			gotIDs := make([]int64, len(c.Posts))
			for i, p := range c.Posts {
				gotIDs[i] = p.ID
			}
			check(t, fmt.Sprintf("country %d posts", c.ID), about, gotIDs, wantIDs)
		}
	}
}

func TestOracleWrites(t *testing.T) {
	setup(t)
	rng := oracleRand(t)
	w := seedWorld(t, rng)
	for step := range oracleRounds {
		mode := trashedMode(rng.IntN(3))
		f := randomFilter(rng, w, mode)
		o := randomOrder(rng)
		q := f.apply(Users.Query())
		target := expectedUsers(w, f, o)
		limit := 0
		if rng.IntN(3) == 0 {
			limit = 1 + rng.IntN(4)
			q = q.OrderBy(o.orders...).Limit(limit)
			target = target[:min(limit, len(target))]
		}
		hit := map[int64]bool{}
		for _, u := range target {
			hit[u.id] = true
		}
		sql, _ := rendered(q.ToSQL())
		var op string
		var affected int64
		apply := func(u *oUser) {}
		switch k := rng.IntN(10); {
		case k < 4:
			v := rng.IntN(21)
			op, affected, apply = fmt.Sprint("set karma ", v), must(q.Update(Users.Karma.Set(v))), func(u *oUser) { u.karma = v }
		case k < 6:
			by := 1 + rng.IntN(3)
			if rng.IntN(2) == 0 {
				op, affected, apply = fmt.Sprint("increment karma by ", by), must(q.Increment(Users.Karma, by)), func(u *oUser) { u.karma += by }
			} else {
				op, affected, apply = fmt.Sprint("decrement karma by ", by), must(q.Decrement(Users.Karma, by)), func(u *oUser) { u.karma -= by }
			}
		case k < 7:
			v := rng.IntN(2) == 0
			op, affected, apply = fmt.Sprint("set active ", v), must(q.Update(Users.Active.Set(v))), func(u *oUser) { u.active = v }
		case k < 9 && mode == withoutTrashed:
			op, affected, apply = "soft delete", must(q.Delete()), func(u *oUser) { u.trashed = true }
		case k < 9 && mode == onlyTrashed:
			op, affected, apply = "restore", must(q.Restore()), func(u *oUser) { u.trashed = false }
		case k < 9 || limit == 0:
			by := 1 + rng.IntN(3)
			op, affected, apply = fmt.Sprint("increment karma by ", by), must(q.Increment(Users.Karma, by)), func(u *oUser) { u.karma += by }
		default:
			op, affected = "force delete", must(q.ForceDelete())
			w.users = slices.DeleteFunc(w.users, func(u oUser) bool { return hit[u.id] })
		}
		about := fmt.Sprintf("step %d: %s where\n  %s\n  limit %d by %s\n  %s", step, op, f.desc, limit, o.desc, sql)
		check(t, "affected rows", about, affected, int64(len(target)))
		for i := range w.users {
			if hit[w.users[i].id] {
				apply(&w.users[i])
			}
		}
		got := must(Users.Query().WithTrashed().OrderBy(Users.ID.Asc()).Get())
		gotRows := make([]string, len(got))
		for i, u := range got {
			gotRows[i] = fmt.Sprintf("%d karma=%d active=%v trashed=%v", u.ID, u.Karma, u.Active, u.DeletedAt != nil)
		}
		wantRows := make([]string, len(w.users))
		for i, u := range w.users {
			wantRows[i] = fmt.Sprintf("%d karma=%d active=%v trashed=%v", u.id, u.karma, u.active, u.trashed)
		}
		check(t, "rows after the write", about, gotRows, wantRows)
	}
}
