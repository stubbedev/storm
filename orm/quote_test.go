package orm

import "testing"

func TestIdentEscapesEmbeddedQuotes(t *testing.T) {
	cases := []struct {
		dialect Dialect
		name    string
		want    string
	}{
		{SQLite, `users.id`, `"users"."id"`},
		{Postgres, `we"ird.co""l`, `"we""ird"."co""""l"`},
		{MySQL, "we`ird.*", "`we``ird`.*"},
		{clickhouseDialect{}, "`", "````"},
	}
	for _, c := range cases {
		b := &SQL{Dialect: c.dialect}
		b.Ident(c.name)
		if got := b.String(); got != c.want {
			t.Errorf("%s Ident(%q) = %s, want %s", c.dialect.Name(), c.name, got, c.want)
		}
	}
	if got, want := quoteIdent(Postgres, `a"b`), `"a""b"`; got != want {
		t.Errorf("quoteIdent = %s, want %s", got, want)
	}
}
