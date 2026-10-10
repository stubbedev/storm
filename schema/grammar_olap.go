package schema

import (
	"cmp"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var settingName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// duckdbType maps blueprint types to DuckDB's SQL. DuckDB is
// Postgres-flavoured: same INTEGER/BIGINT/VARCHAR/TIMESTAMP vocabulary, but
// no serial types (auto-increment columns draw from sequences), native
// ENUM types, a JSON type, and FLOAT[N] arrays for vectors.
func (g grammar) duckdbType(c *Column) (string, error) {
	n := strconv.Itoa
	switch c.Type {
	case TypeString:
		return "varchar(" + n(c.Length) + ")", nil
	case TypeChar:
		return "char(" + n(c.Length) + ")", nil
	case TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return "text", nil
	case TypeTinyInteger, TypeSmallInteger, TypeMediumInteger, TypeInteger:
		return "integer", nil
	case TypeBigInteger:
		return "bigint", nil
	case TypeBoolean:
		return "boolean", nil
	case TypeDecimal:
		return "decimal(" + n(c.Precision) + ", " + n(c.Scale) + ")", nil
	case TypeFloat:
		return floatType(c, "real", "double"), nil
	case TypeDouble:
		return "double", nil
	case TypeDate:
		return "date", nil
	case TypeTime:
		return "time", nil
	case TypeTimeTz:
		return "timetz", nil
	case TypeDateTime, TypeTimestamp:
		return "timestamp", nil
	case TypeDateTimeTz, TypeTimestampTz:
		return "timestamptz", nil
	case TypeYear:
		return "integer", nil
	case TypeJSON, TypeJSONB:
		return "json", nil
	case TypeBinary:
		return "blob", nil
	case TypeUUID:
		return "uuid", nil
	case TypeULID, TypeIPAddress, TypeMACAddress:
		return "varchar(255)", nil
	case TypeEnum:
		return "enum(" + g.literals(c.Allowed) + ")", nil
	case TypeSet:
		return "", g.unsupported(c.bp.table, "create set column "+c.Name, "SET is a MySQL type; use Enum or a list column")
	case TypeGeometry, TypeGeography:
		return "geometry", nil
	case TypeVector:
		return "float[" + n(c.Length) + "]", nil
	}
	return "", g.unknownType(c)
}

// floatType picks single precision for Float(n) with n <= 24, as SQL's
// FLOAT(p) does.
func floatType(c *Column, single, double string) string {
	if c.Precision > 0 && c.Precision <= 24 {
		return single
	}
	return double
}

// clickhouseType maps blueprint types to ClickHouse. Nullable columns wrap
// the type in Nullable(); ClickHouse generates no keys, so auto-increment
// columns are refused (use uuid, ulid or manual keys, as the orm requires).
func (g grammar) clickhouseType(c *Column) (string, error) {
	if err := checkType(c); err != nil {
		return "", err
	}
	table := c.bp.table
	if c.IsAutoIncrement {
		return "", g.unsupported(table, "create auto-incrementing column "+c.Name,
			"ClickHouse does not generate integer keys; use a uuid, ulid or manual key, e.g. UnsignedBigInteger(\""+c.Name+"\").Primary()")
	}
	n := strconv.Itoa
	unsigned := map[bool]string{true: "U", false: ""}[c.IsUnsigned]
	var t string
	switch c.Type {
	case TypeString, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		t = "String"
	case TypeChar:
		t = "FixedString(" + n(c.Length) + ")"
	case TypeTinyInteger:
		t = unsigned + "Int8"
	case TypeSmallInteger, TypeYear:
		t = unsigned + "Int16"
	case TypeMediumInteger, TypeInteger:
		t = unsigned + "Int32"
	case TypeBigInteger:
		t = unsigned + "Int64"
	case TypeBoolean:
		t = "Bool"
	case TypeDecimal:
		t = "Decimal(" + n(c.Precision) + ", " + n(c.Scale) + ")"
	case TypeFloat:
		t = floatType(c, "Float32", "Float64")
	case TypeDouble:
		t = "Float64"
	case TypeDate:
		t = "Date"
	case TypeTime, TypeTimeTz:
		t = "String"
	case TypeDateTime, TypeDateTimeTz:
		t = "DateTime"
		if c.Precision > 0 {
			t = "DateTime64(" + n(c.Precision) + ")"
		}
	case TypeTimestamp, TypeTimestampTz:
		t = "DateTime64(" + n(cmp.Or(c.Precision, 3)) + ")"
	case TypeJSON, TypeJSONB, TypeBinary, TypeUUID, TypeULID, TypeMACAddress, TypeGeometry, TypeGeography:
		t = "String"
	case TypeIPAddress:
		t = "IPv6"
	case TypeEnum:
		enum, err := g.clickhouseEnum(c)
		if err != nil {
			return "", err
		}
		t = enum
	case TypeSet:
		return "", g.unsupported(table, "create set column "+c.Name, "SET is a MySQL type; use Enum or Array(String)")
	case TypeVector:
		if c.IsNullable {
			return "", g.unsupported(table, "create nullable vector column "+c.Name, "ClickHouse arrays cannot be Nullable")
		}
		return "Array(Float64)", nil
	default:
		return "", g.unknownType(c)
	}
	if c.IsNullable {
		return "Nullable(" + t + ")", nil
	}
	return t, nil
}

func (g grammar) clickhouseEnum(c *Column) (string, error) {
	kind := "Enum8"
	switch {
	case len(c.Allowed) > 32767:
		return "", g.unsupported(c.bp.table, "create enum column "+c.Name+" with "+strconv.Itoa(len(c.Allowed))+" values", "Enum16 holds at most 32767 values")
	case len(c.Allowed) > 127:
		kind = "Enum16"
	}
	vals := make([]string, len(c.Allowed))
	for i, v := range c.Allowed {
		vals[i] = g.literal(v) + " = " + strconv.Itoa(i+1)
	}
	return kind + "(" + strings.Join(vals, ", ") + ")", nil
}

func (g grammar) clickhouseColumn(c *Column) (string, error) {
	table := c.bp.table
	switch {
	case c.CharsetName != "" || c.CollationName != "":
		return "", g.unsupported(table, "set the character set or collation of column "+c.Name, "ClickHouse strings are bytes without collation")
	case c.OnUpdateNow:
		return "", g.unsupported(table, "update column "+c.Name+" on every write", "ON UPDATE CURRENT_TIMESTAMP is a MySQL feature")
	case c.Identity != "":
		return "", g.unsupported(table, "make column "+c.Name+" an identity column", "ClickHouse does not generate keys")
	}
	typ, err := g.clickhouseType(c)
	if err != nil {
		return "", err
	}
	s := g.quote(c.Name) + " " + typ
	switch {
	case c.StoredExpr != "":
		s += " MATERIALIZED " + c.StoredExpr
	case c.VirtualExpr != "":
		s += " ALIAS " + c.VirtualExpr
	case c.HasDefault:
		s += " DEFAULT " + g.defaultValue(c)
	}
	if c.CommentText != "" {
		s += " COMMENT " + g.literal(c.CommentText)
	}
	return s, nil
}

// clickhouseIndex renders a data skipping index. A plain Index becomes a
// bloom_filter index, which serves the equality lookups an index is
// declared for (Morphs' type and id included); Algorithm picks another
// TYPE such as minmax or set(100).
func (g grammar) clickhouseIndex(table string, cmd *command) (string, error) {
	op := cmd.kind.String() + " " + cmd.index.IndexName
	switch cmd.kind {
	case cmdIndex:
	case cmdUnique:
		return "", g.unsupported(table, op, "ClickHouse has no unique constraints; ReplacingMergeTree deduplicates by sorting key")
	default:
		return "", g.unsupported(table, op, "ClickHouse has data skipping indexes only; use Index(...).Algorithm(\"tokenbf_v1(...)\") or similar")
	}
	expr := g.list(cmd.columns)
	if len(cmd.columns) > 1 {
		expr = "(" + expr + ")"
	}
	return "INDEX " + g.quote(cmd.index.IndexName) + " " + expr + " TYPE " + cmp.Or(cmd.index.Algo, "bloom_filter") + " GRANULARITY 1", nil
}

// clickhouseStorage renders the ENGINE clause and, for MergeTree engines,
// the sorting key (defaulting to the primary key), partition key and TTL.
func (g grammar) clickhouseStorage(bp *Blueprint, primary []string) (string, error) {
	engineName := cmp.Or(bp.engine, "MergeTree")
	family, _, _ := strings.Cut(engineName, "(")
	o := bp.sorting
	s := " ENGINE = " + engineName
	if !strings.HasSuffix(strings.TrimSpace(family), "MergeTree") {
		if len(o.orderBy) > 0 || o.unsorted || o.partitionBy != "" || o.ttl != "" || primary != nil {
			return "", g.unsupported(bp.table, "declare a primary key, sorting key, partition key or TTL", "engine "+engineName+" is not a MergeTree engine")
		}
	} else {
		switch {
		case o.unsorted && (len(o.orderBy) > 0 || primary != nil):
			return "", g.unsupported(bp.table, "declare Unsorted() together with a sorting or primary key", "ORDER BY tuple() means no sorting key")
		case o.unsorted:
			s += " ORDER BY tuple()"
		case len(o.orderBy) > 0:
			s += " ORDER BY (" + g.list(o.orderBy) + ")"
			if primary != nil && (len(primary) > len(o.orderBy) || !slices.Equal(primary, o.orderBy[:len(primary)])) {
				return "", g.unsupported(bp.table, "use primary key ("+strings.Join(primary, ", ")+") with sorting key ("+strings.Join(o.orderBy, ", ")+")",
					"a MergeTree primary key must be a prefix of the sorting key")
			}
			if primary != nil && len(primary) < len(o.orderBy) {
				s += " PRIMARY KEY (" + g.list(primary) + ")"
			}
		case primary != nil:
			s += " ORDER BY (" + g.list(primary) + ")"
		default:
			return "", g.unsupported(bp.table, "create a "+engineName+" table without a sorting key",
				"declare Primary(...) or OrderBy(...), or Unsorted() for ORDER BY tuple()")
		}
		if o.partitionBy != "" {
			s += " PARTITION BY " + o.partitionBy
		}
		if o.ttl != "" {
			s += " TTL " + o.ttl
		}
	}
	if len(o.settings) > 0 {
		parts := make([]string, len(o.settings))
		for i, st := range o.settings {
			if !settingName.MatchString(st.name) {
				return "", g.unsupported(bp.table, "apply setting "+strconv.Quote(st.name), "setting names are plain identifiers")
			}
			parts[i] = st.name + " = " + g.literal(st.value)
		}
		s += " SETTINGS " + strings.Join(parts, ", ")
	}
	if bp.comment != "" {
		s += " COMMENT " + g.literal(bp.comment)
	}
	return s, nil
}
