package orm

import "fmt"

// Pair is one row of a join as both its models: Left from the query's own
// table, Right from the joined one, nil where a LEFT JOIN found no row.
type Pair[L, R any] struct {
	Left  L
	Right *R
}

// Joined runs q, which joins right's table, and returns every row as a
// Pair hydrated from each table's own columns, so that columns of the same
// name (id, created_at) never mix:
//
//	pairs, err := orm.Joined(Users.Query().LeftJoin(Posts, Posts.UserID.EqCol(Users.ID)), Posts.Table)
//
// q's selects and virtual columns fill Left, and its eager loads and
// Retrieved events run on the Left models; Right gets every column of
// right's table and its Retrieved events. Right's global scopes, soft
// deletes included, do not apply to a join: constrain it in the join's
// conditions. A table joined to itself needs an alias, which Joined has
// none for, so it is refused.
func Joined[L any, LK comparable, R any, RK comparable](q Query[L, LK], right *Table[R, RK]) ([]Pair[L, R], error) {
	ctx := q.context()
	if right.Name == q.table.Name {
		return nil, fmt.Errorf("orm: Joined of %s with itself needs a table alias; select the joined columns with AddSelectAs instead", right.Name)
	}
	q = q.prepared()
	if _, doc, err := q.docstore(ctx); err != nil {
		return nil, err
	} else if doc {
		return nil, &ErrNotTranslatable{Reason: "joins"}
	}
	if len(q.joins) == 0 {
		return nil, fmt.Errorf("orm: Joined on %s: the query joins no table", q.table.Name)
	}
	rows, err := q.query(ctx, func(b *SQL) {
		q.renderSelect(b, func(b *SQL) {
			q.renderColumns(b)
			for _, c := range right.Columns {
				b.Write(", ")
				b.Col(right.Name, c)
			}
		})
	})
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	dests := q.dests()
	var pairs []Pair[L, R]
	for rows.Next() {
		var p Pair[L, R]
		var r R
		found := false
		ptrs := make([]any, 0, len(dests)+len(right.Columns))
		for _, d := range dests {
			ptr := q.table.Ptr(&p.Left, d)
			if ptr == nil {
				return nil, fmt.Errorf("orm: %s has no field for selected column %q", q.table.Name, d)
			}
			ptrs = append(ptrs, nullable{ptr})
		}
		for _, c := range right.Columns {
			ptrs = append(ptrs, presence{nullable{right.Ptr(&r, c)}, &found})
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		q.table.sync(&p.Left)
		if found {
			right.sync(&r)
			p.Right = &r
		}
		pairs = append(pairs, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	lefts := make([]L, len(pairs))
	for i := range pairs {
		lefts[i] = pairs[i].Left
	}
	if err := q.finish(ctx, lefts); err != nil {
		return nil, err
	}
	for i := range pairs {
		pairs[i].Left = lefts[i]
		if pairs[i].Right != nil {
			if err := right.fire(ctx, Retrieved, pairs[i].Right); err != nil {
				return nil, err
			}
		}
	}
	return pairs, nil
}

// presence scans through to dst and records whether the value was
// non-NULL, which tells a joined row from a LEFT JOIN's missing one.
type presence struct {
	dst   nullable
	found *bool
}

func (p presence) Scan(src any) error {
	if src != nil {
		*p.found = true
	}
	return p.dst.Scan(src)
}
