package models_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
)

// planned are the tables grown to a realistic size, which a hot query must
// never read in full.
var planned = []string{"users", "posts"}

// TestQueryPlans runs the statements the builders emit on their hot paths
// against realistically sized tables and fails when the engine plans a
// full scan of one of them: key lookups, WhereKey, keyset pages, eager
// loads by foreign key (per-parent windows and through relations
// included) and limited mutations through their key subquery.
func TestQueryPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("grows tables to thousands of rows")
	}
	ctx := setup(t)
	c := must(orm.Connection(ctx, orm.DefaultConnection))
	growTables(t, ctx, c)
	errRollBack := errors.New("roll back")
	userOrders := [][]orm.Order[User]{
		{Users.ID.Asc()},
		{Users.CountryID.Asc(), Users.ID.Asc()},
		{Users.CountryID.Desc(), Users.ID.Desc()},
		{Users.CountryID.Asc().NullsLast(), Users.ID.Asc()},
	}
	postOrders := [][]orm.Order[Post]{
		{Posts.UserID.Asc(), Posts.ID.Asc()},
		{Posts.UserID.Desc(), Posts.ID.Desc()},
	}
	userCursors := keysetCursors(Users.Query(), 4000, userOrders)
	postCursors := keysetCursors(Posts.Query(), 12000, postOrders)
	for _, hot := range []struct {
		name string
		run  func(ctx context.Context)
	}{
		{"find by key", func(context.Context) { must(Users.Find(4321)) }},
		{"whereKey", func(context.Context) { must(Users.Query().WhereKey(1, 2, 3, 4000).Get()) }},
		{"eager load by foreign key", func(context.Context) { must(Users.Query().WhereKey(1, 2, 3).With(UserPosts).Get()) }},
		{"eager limit per parent", func(context.Context) {
			must(Users.Query().WhereKey(1, 2, 3).With(UserPosts, func(q PostQuery) PostQuery { return q.OrderBy(Posts.ID.Desc()).Limit(2) }).Get())
		}},
		{"through eager load", func(context.Context) { must(Countries.Query().WhereKey(2).With(CountryPosts).Get()) }},
		{"belongsTo eager load", func(context.Context) { must(Posts.Query().WhereKey(1, 2, 3).With(PostAuthor).Get()) }},
		{"latest of many eager load", func(context.Context) { must(Users.Query().WhereKey(1, 2, 3).With(UserLatestPost).Get()) }},
		{"relation existence", func(context.Context) {
			must(Users.Query().WhereKey(1, 2, 3).WhereHas(UserPosts, Published).Get())
			must(Users.Query().WhereKey(1, 2, 3).WhereDoesntHave(UserPosts).Get())
			must(Users.Query().WhereKey(1, 2, 3).HasCount(UserPosts, orm.Gte, 2).Get())
			must(Users.Query().WhereKey(1, 2, 3).WhereHas(UserLatestPost, Published).Get())
			must(Users.Query().WhereKey(1, 2, 3).WhereHas(UserRoles).Get())
			must(Users.Query().WhereKey(1, 2, 3).WhereHas(UserCountry).Get())
			must(Posts.Query().WhereKey(1, 2, 3).WhereHas(PostAuthor, Active).Get())
		}},
		{"relation aggregates", func(context.Context) {
			users := must(Users.Query().WhereKey(1, 2, 3).
				WithCount(UserPosts, Users.PostsCount).
				WithSum(UserPosts, Posts.Views, Users.PostViews).
				WithAvg(UserPosts, Posts.Views, Users.AvgPostViews).
				WithMax(UserPosts, Posts.Title, Users.LatestPostTitle).
				WithExists(UserPosts, Users.HasAvatar).
				Get())
			ok(orm.LoadCount(ctx, users, UserPosts, Users.PostsCount))
		}},
		{"filtered aggregates", func(context.Context) {
			posts := Posts.Query().Where(Posts.UserID.Eq(42))
			must(posts.Count())
			must(posts.Exists())
			must(posts.Sum(Posts.Views))
			must(posts.Max(Posts.Views))
			must(posts.Pluck(Posts.ID))
		}},
		{"filtered pages", func(context.Context) {
			byCountry := Users.Query().Where(Users.CountryID.Eq(2)).OrderBy(Users.ID.Asc())
			must(byCountry.Paginate(3, 2))
			must(byCountry.SimplePaginate(3, 2))
			ok(byCountry.ChunkByID(4, func([]User) error { return nil }))
		}},
		{"keyset pages", func(context.Context) {
			walkKeyset(Users.Query(), userOrders, userCursors)
			walkKeyset(Posts.Query(), postOrders, postCursors)
		}},
		{"relation writes", func(ctx context.Context) {
			err := orm.Transaction(ctx, func(ctx context.Context) error {
				must(Users.WithContext(ctx).Query().WhereKey(1, 2, 3).WhereHas(UserPosts).Update(Users.Karma.Set(1)))
				must(Users.WithContext(ctx).Query().WhereKey(4, 5).Delete())
				must(Users.WithContext(ctx).Query().OnlyTrashed().WhereKey(4, 5).Restore())
				return errRollBack
			})
			if !errors.Is(err, errRollBack) {
				t.Fatal(err)
			}
		}},
		{"limited mutations", func(ctx context.Context) {
			err := orm.Transaction(ctx, func(ctx context.Context) error {
				must(Users.WithContext(ctx).Where(Users.ID.Gt(4990)).OrderBy(Users.ID.Asc()).Limit(3).Update(Users.Karma.Set(1)))
				must(Posts.WithContext(ctx).Where(Posts.ID.Gt(19990)).OrderBy(Posts.ID.Asc()).Limit(3).Delete())
				return errRollBack
			})
			if !errors.Is(err, errRollBack) {
				t.Fatal(err)
			}
		}},
	} {
		var stmts []orm.QueryEvent
		stop := orm.Listen(func(e orm.QueryEvent) {
			if verb, _, _ := strings.Cut(strings.TrimSpace(e.SQL), " "); slices.Contains([]string{"SELECT", "UPDATE", "DELETE", "WITH"}, strings.ToUpper(verb)) {
				stmts = append(stmts, e)
			}
		})
		hot.run(ctx)
		stop()
		if len(stmts) == 0 {
			t.Fatalf("%s ran no statement", hot.name)
		}
		for _, e := range stmts {
			for _, table := range fullScans(t, ctx, c, e) {
				t.Errorf("%s: %s scans %s in full:\n%s", hot.name, c.Dialect.Name(), table, e.SQL)
			}
			produced, read := examined(t, ctx, c, e)
			for _, table := range planned {
				if n := read[table]; n > 10*produced+100 {
					t.Errorf("%s: %s reads %.0f rows of %s to produce %.0f:\n%s", hot.name, c.Dialect.Name(), n, table, produced, e.SQL)
				}
			}
		}
	}
}

func keysetCursors[M any, K comparable](q orm.Query[M, K], depth int, orders [][]orm.Order[M]) []string {
	cursors := make([]string, len(orders))
	for i, o := range orders {
		cursors[i] = must(q.OrderBy(o...).CursorPaginate(depth, "")).NextCursor
	}
	return cursors
}

func walkKeyset[M any, K comparable](q orm.Query[M, K], orders [][]orm.Order[M], deep []string) {
	for i, o := range orders {
		ordered := q.OrderBy(o...)
		first := must(ordered.CursorPaginate(5, ""))
		must(ordered.CursorPaginate(5, first.NextCursor))
		back := must(ordered.CursorPaginate(5, deep[i]))
		must(ordered.CursorPaginate(5, back.PrevCursor))
	}
}

// growTables adds thousands of users and posts, most users without a
// country, so that a foreign key lookup is selective, and refreshes the
// planner's statistics.
func growTables(t *testing.T, ctx context.Context, c orm.Conn) {
	t.Helper()
	const users, postsPerUser, batch = 5000, 4, 500
	for from := 10; from < users; from += batch {
		var us, ps []map[string]any
		for id := from; id < min(from+batch, users); id++ {
			u := map[string]any{"id": id, "name": fmt.Sprint("user ", id), "email": fmt.Sprintf("user%d@example.com", id), "active": true, "karma": id % 100}
			if id%500 == 0 {
				u["country_id"] = 2
			}
			us = append(us, u)
			for p := range postsPerUser {
				ps = append(ps, map[string]any{"id": id*postsPerUser + p, "user_id": id, "title": fmt.Sprint("post ", id, p), "published": true, "views": p})
			}
		}
		ok(orm.From("users").Insert(us...))
		ok(orm.From("posts").Insert(ps...))
	}
	analyze := map[string][]string{
		"sqlite":   {"ANALYZE"},
		"postgres": {"ANALYZE users", "ANALYZE posts"},
		"mysql":    {"ANALYZE TABLE users, posts"},
		"mariadb":  {"ANALYZE TABLE users, posts"},
	}[c.Dialect.Name()]
	for _, stmt := range analyze {
		rows, err := c.DB.QueryContext(ctx, stmt)
		ok(err)
		ok(rows.Close())
	}
}

var sqliteScan = regexp.MustCompile(`^SCAN (\w+)`)

// fullScans explains e and names the planned tables its plan reads in
// full. On SQLite a scan in key order that a LIMIT stops, with no sort
// before it, reads only as many rows as it returns, so it does not count.
func fullScans(t *testing.T, ctx context.Context, c orm.Conn, e orm.QueryEvent) []string {
	t.Helper()
	var scanned []string
	note := func(table string) {
		if slices.Contains(planned, table) && !slices.Contains(scanned, table) {
			scanned = append(scanned, table)
		}
	}
	switch c.Dialect.Name() {
	case "sqlite":
		rows := explain(t, ctx, c.DB, "EXPLAIN QUERY PLAN "+e.SQL, e.Args)
		sorted := slices.ContainsFunc(rows, func(row map[string]any) bool {
			return strings.HasPrefix(fmt.Sprint(row["detail"]), "USE TEMP B-TREE FOR ORDER BY")
		})
		bounded := !sorted && strings.Contains(e.SQL, " LIMIT ")
		for _, row := range rows {
			if m := sqliteScan.FindStringSubmatch(fmt.Sprint(row["detail"])); m != nil && !bounded {
				note(m[1])
			}
		}
	case "postgres":
		rows := explain(t, ctx, c.DB, "EXPLAIN (FORMAT JSON) "+e.SQL, e.Args)
		var plans []struct{ Plan json.RawMessage }
		ok(json.Unmarshal([]byte(fmt.Sprint(rows[0]["QUERY PLAN"])), &plans))
		var walk func(raw json.RawMessage)
		walk = func(raw json.RawMessage) {
			var node struct {
				NodeType string            `json:"Node Type"`
				Relation string            `json:"Relation Name"`
				Plans    []json.RawMessage `json:"Plans"`
			}
			ok(json.Unmarshal(raw, &node))
			if node.NodeType == "Seq Scan" {
				note(node.Relation)
			}
			for _, p := range node.Plans {
				walk(p)
			}
		}
		walk(plans[0].Plan)
	case "mysql", "mariadb":
		for _, row := range explain(t, ctx, c.DB, "EXPLAIN "+e.SQL, e.Args) {
			if fmt.Sprint(row["type"]) == "ALL" {
				note(fmt.Sprint(row["table"]))
			}
		}
	default:
		t.Fatalf("no plan reader for %s", c.Dialect.Name())
	}
	return scanned
}

func explain(t *testing.T, ctx context.Context, db orm.DB, stmt string, args []any) []map[string]any {
	t.Helper()
	rows, err := db.QueryContext(ctx, stmt, args...)
	if err != nil {
		t.Fatalf("explaining %s: %v", stmt, err)
	}
	defer rows.Close()
	cols := must(rows.Columns())
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		ok(rows.Scan(ptrs...))
		row := map[string]any{}
		for i, col := range cols {
			if b, isBytes := vals[i].([]byte); isBytes {
				vals[i] = string(b)
			}
			row[col] = vals[i]
		}
		out = append(out, row)
	}
	ok(rows.Err())
	return out
}

var mysqlAccess = regexp.MustCompile(`-> [^:]*? on (\w+)\b.*\(actual time=\S+ rows=([\d.e+]+) loops=(\d+)\)`)

func examined(t *testing.T, ctx context.Context, c orm.Conn, e orm.QueryEvent) (produced float64, read map[string]float64) {
	t.Helper()
	read = map[string]float64{}
	add := func(table string, rows float64) {
		if slices.Contains(planned, table) {
			read[table] += rows
		}
	}
	verb, _, _ := strings.Cut(strings.TrimSpace(e.SQL), " ")
	selects := slices.Contains([]string{"SELECT", "WITH"}, strings.ToUpper(verb))
	rolledBack(t, ctx, func(tx *sql.Tx) {
		if !selects {
			produced = float64(must(must(tx.ExecContext(ctx, e.SQL, e.Args...)).RowsAffected()))
			return
		}
		for _, row := range explain(t, ctx, tx, "SELECT COUNT(*) AS n FROM ("+e.SQL+") produced", e.Args) {
			produced = float64(must(strconv.ParseInt(fmt.Sprint(row["n"]), 10, 64)))
		}
	})
	switch c.Dialect.Name() {
	case "postgres":
		rolledBack(t, ctx, func(tx *sql.Tx) {
			rows := explain(t, ctx, tx, "EXPLAIN (ANALYZE, FORMAT JSON) "+e.SQL, e.Args)
			var plans []struct{ Plan json.RawMessage }
			ok(json.Unmarshal([]byte(fmt.Sprint(rows[0]["QUERY PLAN"])), &plans))
			var walk func(raw json.RawMessage)
			walk = func(raw json.RawMessage) {
				var node struct {
					Relation string            `json:"Relation Name"`
					Rows     float64           `json:"Actual Rows"`
					Loops    float64           `json:"Actual Loops"`
					Filtered float64           `json:"Rows Removed by Filter"`
					Rechecks float64           `json:"Rows Removed by Index Recheck"`
					Plans    []json.RawMessage `json:"Plans"`
				}
				ok(json.Unmarshal(raw, &node))
				add(node.Relation, (node.Rows+node.Filtered+node.Rechecks)*node.Loops)
				for _, p := range node.Plans {
					walk(p)
				}
			}
			walk(plans[0].Plan)
		})
	case "mysql":
		if !selects {
			return produced, read
		}
		for _, row := range explain(t, ctx, c.DB, "EXPLAIN ANALYZE "+e.SQL, e.Args) {
			for _, m := range mysqlAccess.FindAllStringSubmatch(fmt.Sprint(row["EXPLAIN"]), -1) {
				add(m[1], must(strconv.ParseFloat(m[2], 64))*must(strconv.ParseFloat(m[3], 64)))
			}
		}
	case "mariadb":
		rolledBack(t, ctx, func(tx *sql.Tx) {
			rows := explain(t, ctx, tx, "ANALYZE FORMAT=JSON "+e.SQL, e.Args)
			var plan any
			ok(json.Unmarshal([]byte(fmt.Sprint(rows[0]["ANALYZE"])), &plan))
			var walk func(v any)
			walk = func(v any) {
				switch v := v.(type) {
				case map[string]any:
					if table, isTable := v["table_name"].(string); isTable {
						rows, _ := v["r_rows"].(float64)
						loops, _ := v["r_loops"].(float64)
						add(table, rows*loops)
					}
					for _, k := range slices.Sorted(maps.Keys(v)) {
						walk(v[k])
					}
				case []any:
					for _, x := range v {
						walk(x)
					}
				}
			}
			walk(plan)
		})
	}
	return produced, read
}

func rolledBack(t *testing.T, ctx context.Context, run func(*sql.Tx)) {
	t.Helper()
	tx := must(mainDB.BeginTx(ctx, nil))
	defer func() { ok(tx.Rollback()) }()
	run(tx)
}
