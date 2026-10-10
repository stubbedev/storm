// Package models is an example schema exercising every relation type.
// Columns are generated into orm_gen.go by cmd/ormgen.
package models

import (
	"time"

	"github.com/stubbedev/eloquent-go/orm"
)

//go:generate go run github.com/stubbedev/eloquent-go/cmd/ormgen

//orm:table countries
type Country struct {
	orm.Model
	ID   int64  `db:"id"`
	Name string `db:"name"`

	Posts []Post // hasManyThrough users
}

// Settings is stored as JSON (an 'array' cast).
type Settings struct {
	Theme  string   `json:"theme"`
	Labels []string `json:"labels"`
}

//orm:table users soft_deletes
type User struct {
	orm.Model
	ID        int64              `db:"id"`
	CountryID int64              `db:"country_id,nullzero"`
	Name      string             `db:"name"`
	Email     string             `db:"email"`
	Active    bool               `db:"active"`
	Karma     int                `db:"karma"`
	Settings  orm.JSON[Settings] `db:"settings"`
	CreatedAt time.Time          `db:"created_at"`
	UpdatedAt time.Time          `db:"updated_at"`
	DeletedAt *time.Time         `db:"deleted_at"`

	// Virtual columns: filled by WithCount / WithSum / AddSelectAs.
	PostsCount      int64   `db:"posts_count,virtual"`
	PostViews       int64   `db:"post_views,virtual"`
	HasAvatar       bool    `db:"has_avatar,virtual"`
	LatestPostTitle string  `db:"latest_post_title,virtual"`
	AvgPostViews    float64 `db:"avg_post_views,virtual"`

	Country    *Country
	Posts      []Post
	LatestPost *Post
	Roles      []Role
	Avatar     *Image
}

//orm:table posts
type Post struct {
	orm.Model
	ID        int64     `db:"id"`
	UserID    int64     `db:"user_id"`
	Title     string    `db:"title"`
	Published bool      `db:"published"`
	Views     int64     `db:"views"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`

	Author   *User
	Comments []Comment
	Tags     []Tag
}

//orm:table videos
type Video struct {
	orm.Model
	ID    int64  `db:"id"`
	Title string `db:"title"`

	Comments []Comment
	Tags     []Tag
}

//orm:table comments
type Comment struct {
	orm.Model
	ID              int64     `db:"id"`
	CommentableID   int64     `db:"commentable_id"`
	CommentableType string    `db:"commentable_type"`
	Body            string    `db:"body"`
	CreatedAt       time.Time `db:"created_at"`
	UpdatedAt       time.Time `db:"updated_at"`

	Commentable any // *Post or *Video (morphTo)
}

//orm:table roles
type Role struct {
	orm.Model
	ID   int64  `db:"id"`
	Name string `db:"name"`

	Pivot RoleUser // filled by UserRoles.WithPivot
}

// RoleUser is the users<->roles pivot, a typed model with extra columns.
//
//orm:table role_user schema=RoleUsers
type RoleUser struct {
	orm.Model
	UserID    int64     `db:"user_id"`
	RoleID    int64     `db:"role_id"`
	GrantedBy string    `db:"granted_by"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

//orm:table tags
type Tag struct {
	orm.Model
	ID   int64  `db:"id"`
	Name string `db:"name"`

	Posts []Post
}

//orm:table taggables timestamps=false
type Taggable struct {
	orm.Model
	TagID        int64  `db:"tag_id"`
	TaggableID   int64  `db:"taggable_id"`
	TaggableType string `db:"taggable_type"`
}

//orm:table images
type Image struct {
	orm.Model
	ID            int64  `db:"id"`
	ImageableID   int64  `db:"imageable_id"`
	ImageableType string `db:"imageable_type"`
	URL           string `db:"url"`
}

// AuditEntry lives in another database with UUID keys.
//
//orm:table audit_log connection=audit key_type=uuid timestamps=false
type AuditEntry struct {
	orm.Model
	ID     string `db:"id"`
	Action string `db:"action"`
}
