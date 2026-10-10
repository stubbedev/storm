package models_test

import (
	"strings"
	"testing"

	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
)

// TestCookbookSnippets runs cookbook snippets not covered elsewhere, so the
// docs can't drift from the API.
func TestCookbookSnippets(t *testing.T) {
	ctx := setup(t)
	alice := must(Users.Find(ctx, 1))
	eq(t, "orm.Has composes", must(Users.Where(orm.Or(orm.Has(UserPosts), Users.Karma.Gt(90))).Count(ctx)), int64(3))
	eq(t, "max time", must(Posts.Query().Max(ctx, Posts.CreatedAt)).Format("2006-01-02"), "2026-03-05")
	alice.Karma++
	eq(t, "isDirty columns", Users.IsDirty(&alice, Users.Karma, Users.Name), true)
	sqlText, _ := rendered(Users.Where(Users.Karma.Gt(10)).ToSQLFor(orm.Postgres))
	eq(t, "toSQLFor", strings.Contains(sqlText, `"users"."karma" > $1`), true)
	eq(t, "fresh", must(Posts.Fresh(ctx, &Post{ID: 1})).Title, "Generics in Go 1.27")
	ok(UserPosts.SaveMany(ctx, &alice, []Post{{Title: "saved via relation"}}))
	eq(t, "saveMany", must(UserPosts.Of(&alice).Count(ctx)), int64(4))
	must(Posts.Where(Posts.ID.Eq(1)).Decrement(ctx, Posts.Views, 1, Posts.Title.Set("edited")))
	eq(t, "decrement extra", must(Posts.Find(ctx, 1)).Title, "edited")
	ok(Users.SaveQuietly(ctx, &alice))
	eq(t, "chained builders", len(must(Users.Query().OrderBy(Users.Name.Asc()).Latest().InRandomOrder().Reorder(Users.ID.Desc()).Limit(10).Offset(1).Distinct().Get(ctx))), 3)
}
