package main

import (
	"context"

	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

func init() {
	schema.RegisterSeeder(schema.Seeder{
		Name: "Countries",
		Run: func(ctx context.Context) error {
			return orm.From("countries").Insert(ctx,
				map[string]any{"id": 1, "name": "Denmark"},
				map[string]any{"id": 2, "name": "Norway"},
			)
		},
	})
}
