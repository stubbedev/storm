package models

import "github.com/stubbedev/storm/orm"

// Relations — the equivalent of Eloquent's relationship methods.
var (
	UserPosts = orm.HasMany(Users.ID, Posts.UserID,
		func(u *User, ps []Post) { u.Posts = ps })

	UserLatestPost = orm.HasOne(Users.ID, Posts.UserID,
		func(u *User, p *Post) { u.LatestPost = p }).OfMany(Posts.ID, true) // latestOfMany

	UserCountry = orm.BelongsTo(Users.CountryID, Countries.ID,
		func(u *User, c *Country) { u.Country = c })

	UserRoles = orm.BelongsToMany(Users.ID, RoleUsers.UserID, Roles.ID, RoleUsers.RoleID,
		func(u *User, rs []Role) { u.Roles = rs }).
		WithPivot(func(r *Role, p RoleUser) { r.Pivot = p })

	UserAvatar = orm.MorphOne(Users.ID, Images.ImageableID, Images.ImageableType,
		func(u *User, i *Image) { u.Avatar = i })

	PostAuthor = orm.BelongsTo(Posts.UserID, Users.ID,
		func(p *Post, u *User) { p.Author = u }).
		WithDefault(func(*Post) *User { return &User{Name: "Guest Author"} })

	PostComments = orm.MorphMany(Posts.ID, Comments.CommentableID, Comments.CommentableType,
		func(p *Post, cs []Comment) { p.Comments = cs })

	VideoComments = orm.MorphMany(Videos.ID, Comments.CommentableID, Comments.CommentableType,
		func(v *Video, cs []Comment) { v.Comments = cs })

	CommentCommentable = orm.MorphTo(Comments.CommentableID, Comments.CommentableType,
		func(c *Comment, owner any) { c.Commentable = owner },
		orm.Target(Posts.ID), orm.Target(Videos.ID))

	PostTags = orm.MorphToMany(Posts.ID, Taggables.TaggableID, Taggables.TaggableType, Tags.ID, Taggables.TagID,
		func(p *Post, ts []Tag) { p.Tags = ts })

	TagPosts = orm.MorphedByMany(Tags.ID, Taggables.TagID, Posts.ID, Taggables.TaggableID, Taggables.TaggableType,
		func(t *Tag, ps []Post) { t.Posts = ps })

	CountryPosts = orm.HasManyThrough(Countries.ID, Users.CountryID, Users.ID, Posts.UserID,
		func(c *Country, ps []Post) { c.Posts = ps })
)

// Local scopes.

func Active(q orm.Query[User]) orm.Query[User] { return q.Where(Users.Active.Eq(true)) }

func Published(q orm.Query[Post]) orm.Query[Post] { return q.Where(Posts.Published.Eq(true)) }

func Popular(minViews int64) func(orm.Query[Post]) orm.Query[Post] {
	return func(q orm.Query[Post]) orm.Query[Post] { return q.Where(Posts.Views.Gte(minViews)) }
}
