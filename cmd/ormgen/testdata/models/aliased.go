package models

import storm "github.com/stubbedev/storm/orm"

//orm:table aliases
type Alias struct {
	storm.Model
	ID   int64                  `db:"id"`
	Tags storm.JSON[[]string]   `db:"tags"`
	Vec  storm.Vector[storm.D3] `db:"vec"`
}
