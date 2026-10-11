// Package models holds a model for every ormgen option and tag.
package models

import (
	"database/sql"
	"time"

	"github.com/stubbedev/storm/cmd/ormgen/testdata/go-kinds"
	"github.com/stubbedev/storm/orm"
)

// Timestamps is embedded for its columns.
type Timestamps struct {
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// UserID is a named integer key.
type UserID int64

//orm:table users soft_deletes time_precision=3 morph=user connection=main
type User struct {
	orm.Model
	Timestamps
	ID        UserID     `db:"id"`
	CountryID int64      `db:"country_id, nullzero"`
	Kind      kinds.Kind `db:"kind"`
	DeletedAt *time.Time `db:"deleted_at"`
	Posts     int64      `db:"posts_count,virtual"`
	Meta      orm.JSON[struct {
		A int `json:"a"`
	}] `db:"meta"`
	Ignored string `db:"-"`
	Related []Fingerprinted
}

// Fingerprint is a named string key.
type Fingerprint string

//orm:table	fingerprints timestamps=false soft_deletes=false schema=Prints
type Fingerprinted struct {
	orm.Model
	Print     Fingerprint `db:"print,pk"`
	CreatedAt time.Time   `db:"created_at"`
}

//orm:table nullables
type Nullable struct {
	orm.Model
	ID sql.NullInt64 `db:"id"`
}

//orm:table tokens key=token key_type=uuid
type Token struct {
	orm.Model
	Token string `db:"token"`
	Value int    `db:"value"`
}

//orm:table counters key_type=increments
type Counter struct {
	orm.Model
	ID uint32 `db:"id"`
}

//orm:table logs
type Log struct {
	orm.Model
	Line string `db:"line"`
}
