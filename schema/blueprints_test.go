package schema

func flightsBlueprint(t *Blueprint) {
	t.ID()
	t.ForeignID("airline_id").Constrained().CascadeOnDelete()
	t.String("name", 100).Comment("flight name")
	t.Char("code", 3).Unique()
	t.Decimal("price", 8, 2).Default(0)
	t.Boolean("active").Default(true)
	t.Enum("status", "scheduled", "departed")
	t.JSON("meta").Nullable()
	t.UUID("ref").Index()
	t.TimestampTz("departs_at").UseCurrent()
	t.Integer("seats").Unsigned()
	t.Integer("price_cents").StoredAs("price * 100")
	t.Text("notes")
	t.Text("summary").Nullable()
	t.Timestamps()
	t.SoftDeletes()
	t.Index("airline_id", "departs_at").Name("flights_airline_departs").Algorithm("btree")
}

func alterBlueprint(t *Blueprint) {
	t.String("gate", 10).Nullable()
	t.String("name", 200).Change()
	t.RenameColumn("notes", "remarks")
	t.DropColumn("ref")
	t.RenameIndex("flights_code_unique", "flights_code_uq")
	t.DropForeignOn("airline_id")
	t.ForeignID("airline_id").Nullable().Change()
	t.Foreign("airline_id").References("id").On("carriers").NullOnDelete()
}
