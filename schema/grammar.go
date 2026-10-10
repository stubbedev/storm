package schema

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Column types understood by every grammar.
const (
	TypeString        = "string"
	TypeChar          = "char"
	TypeTinyText      = "tinyText"
	TypeText          = "text"
	TypeMediumText    = "mediumText"
	TypeLongText      = "longText"
	TypeTinyInteger   = "tinyInteger"
	TypeSmallInteger  = "smallInteger"
	TypeMediumInteger = "mediumInteger"
	TypeInteger       = "integer"
	TypeBigInteger    = "bigInteger"
	TypeBoolean       = "boolean"
	TypeDecimal       = "decimal"
	TypeFloat         = "float"
	TypeDouble        = "double"
	TypeDate          = "date"
	TypeTime          = "time"
	TypeTimeTz        = "timeTz"
	TypeDateTime      = "dateTime"
	TypeDateTimeTz    = "dateTimeTz"
	TypeTimestamp     = "timestamp"
	TypeTimestampTz   = "timestampTz"
	TypeYear          = "year"
	TypeJSON          = "json"
	TypeJSONB         = "jsonb"
	TypeBinary        = "binary"
	TypeUUID          = "uuid"
	TypeULID          = "ulid"
	TypeIPAddress     = "ipAddress"
	TypeMACAddress    = "macAddress"
	TypeEnum          = "enum"
	TypeSet           = "set"
	TypeGeometry      = "geometry"
	TypeGeography     = "geography"
	TypeVector        = "vector"
)

// UnsupportedError reports a schema operation the connection's engine
// cannot express. Nothing of the blueprint was executed.
type UnsupportedError struct {
	Engine    string // sqlite, postgres, mysql, mariadb, duckdb or clickhouse
	Table     string // empty for connection-wide operations
	Operation string
	Reason    string
}

func (e *UnsupportedError) Error() string {
	if e.Table == "" {
		return fmt.Sprintf("schema: %s cannot %s: %s", e.Engine, e.Operation, e.Reason)
	}
	return fmt.Sprintf("schema: %s cannot %s on table %s: %s", e.Engine, e.Operation, e.Table, e.Reason)
}

// engine is the database engine a grammar compiles for.
type engine string

const (
	engineSQLite     engine = "sqlite"
	enginePostgres   engine = "postgres"
	engineMySQL      engine = "mysql"
	engineMariaDB    engine = "mariadb"
	engineDuckDB     engine = "duckdb"
	engineClickHouse engine = "clickhouse"
)

// errRebuild signals that SQLite needs a table rebuild for an alteration
// it cannot express with ALTER TABLE (as Laravel does).
var errRebuild = errors.New("schema: sqlite table rebuild required")

// grammar compiles blueprints for one database engine.
type grammar struct{ engine engine }

// grammarFor maps an orm dialect name to its grammar; MariaDB is MySQL's
// grammar with MariaDB's differences.
func grammarFor(dialect string) (grammar, error) {
	switch e := engine(dialect); e {
	case engineSQLite, enginePostgres, engineMySQL, engineMariaDB, engineDuckDB, engineClickHouse:
		return grammar{engine: e}, nil
	}
	return grammar{}, fmt.Errorf("schema: no grammar for dialect %q", dialect)
}

func (g grammar) mysqlFamily() bool { return g.engine == engineMySQL || g.engine == engineMariaDB }

// dialect is the key introspection queries are stored under; MariaDB
// shares MySQL's information_schema.
func (g grammar) dialect() engine {
	if g.engine == engineMariaDB {
		return engineMySQL
	}
	return g.engine
}

func (g grammar) unsupported(table, op, reason string) error {
	return &UnsupportedError{Engine: string(g.engine), Table: table, Operation: op, Reason: reason}
}

// pick chooses between the SQLite, Postgres (and DuckDB) and MySQL spellings.
func (g grammar) pick(sqlite, postgres, mysql string) string {
	switch {
	case g.mysqlFamily():
		return mysql
	case g.engine == enginePostgres, g.engine == engineDuckDB:
		return postgres
	}
	return sqlite
}

func (g grammar) quote(id string) string {
	if g.mysqlFamily() || g.engine == engineClickHouse {
		return "`" + strings.ReplaceAll(id, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

func (g grammar) list(ids []string) string {
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = g.quote(id)
	}
	return strings.Join(q, ", ")
}

// transactionalDDL reports whether DDL can be rolled back (Laravel wraps
// migrations in a transaction on such engines).
func (g grammar) transactionalDDL() bool {
	return g.engine == engineSQLite || g.engine == enginePostgres || g.engine == engineDuckDB
}

// identity reports whether a Postgres column is an identity column: asked
// for explicitly, or an auto-increment column with a starting value.
func identity(c *Column) bool { return c.Identity != "" || c.IsAutoIncrement && c.StartingValue != 0 }

func (g grammar) typeOf(c *Column) (string, error) {
	if err := checkType(c); err != nil {
		return "", err
	}
	switch g.engine {
	case engineDuckDB:
		return g.duckdbType(c)
	case engineClickHouse:
		return g.clickhouseType(c)
	}
	n := strconv.Itoa
	pgInt := func(base, serial string) string {
		if g.engine == enginePostgres && c.IsAutoIncrement && !c.Changed && !identity(c) {
			return serial
		}
		return base
	}
	switch c.Type {
	case TypeString:
		return g.pick("varchar", "varchar("+n(c.Length)+")", "varchar("+n(c.Length)+")"), nil
	case TypeChar:
		return g.pick("varchar", "char("+n(c.Length)+")", "char("+n(c.Length)+")"), nil
	case TypeTinyText:
		return g.pick("text", "varchar(255)", "tinytext"), nil
	case TypeText:
		return "text", nil
	case TypeMediumText:
		return g.pick("text", "text", "mediumtext"), nil
	case TypeLongText:
		return g.pick("text", "text", "longtext"), nil
	case TypeTinyInteger:
		return g.pick("integer", pgInt("smallint", "smallserial"), "tinyint"), nil
	case TypeSmallInteger:
		return g.pick("integer", pgInt("smallint", "smallserial"), "smallint"), nil
	case TypeMediumInteger:
		return g.pick("integer", pgInt("integer", "serial"), "mediumint"), nil
	case TypeInteger:
		return g.pick("integer", pgInt("integer", "serial"), "int"), nil
	case TypeBigInteger:
		return g.pick("integer", pgInt("bigint", "bigserial"), "bigint"), nil
	case TypeBoolean:
		return g.pick("tinyint(1)", "boolean", "tinyint(1)"), nil
	case TypeDecimal:
		return g.pick("numeric", "decimal", "decimal") + "(" + n(c.Precision) + ", " + n(c.Scale) + ")", nil
	case TypeFloat:
		return g.pick("float", "float("+n(c.Precision)+")", "float("+n(c.Precision)+")"), nil
	case TypeDouble:
		return g.pick("double", "double precision", "double"), nil
	case TypeDate:
		return "date", nil
	case TypeTime, TypeTimeTz, TypeDateTime, TypeDateTimeTz, TypeTimestamp, TypeTimestampTz:
		p, mysqlP := n(c.Precision), ""
		if c.Precision > 0 {
			mysqlP = "(" + p + ")"
		}
		return map[string]string{
			TypeTime:        g.pick("time", "time("+p+") without time zone", "time"+mysqlP),
			TypeTimeTz:      g.pick("time", "time("+p+") with time zone", "time"+mysqlP),
			TypeDateTime:    g.pick("datetime", "timestamp("+p+") without time zone", "datetime"+mysqlP),
			TypeDateTimeTz:  g.pick("datetime", "timestamp("+p+") with time zone", "datetime"+mysqlP),
			TypeTimestamp:   g.pick("datetime", "timestamp("+p+") without time zone", "timestamp"+mysqlP),
			TypeTimestampTz: g.pick("datetime", "timestamp("+p+") with time zone", "timestamp"+mysqlP),
		}[c.Type], nil
	case TypeYear:
		return g.pick("integer", "integer", "year"), nil
	case TypeJSON:
		return g.pick("text", "json", "json"), nil
	case TypeJSONB:
		return g.pick("text", "jsonb", "json"), nil
	case TypeBinary:
		return g.pick("blob", "bytea", "blob"), nil
	case TypeUUID:
		return g.pick("varchar", "uuid", "char(36)"), nil
	case TypeULID:
		return g.pick("varchar", "char(26)", "char(26)"), nil
	case TypeIPAddress:
		return g.pick("varchar", "inet", "varchar(45)"), nil
	case TypeMACAddress:
		return g.pick("varchar", "macaddr", "varchar(17)"), nil
	case TypeEnum:
		return g.pick("varchar", "varchar(255)", "enum("+g.literals(c.Allowed)+")"), nil
	case TypeSet:
		if g.mysqlFamily() {
			return "set(" + g.literals(c.Allowed) + ")", nil
		}
		return "", g.unsupported(c.bp.table, "create set column "+c.Name, "SET is a MySQL type; use Enum or a JSON array")
	case TypeGeometry, TypeGeography:
		return g.spatialType(c)
	case TypeVector:
		return g.pick("blob", "vector("+n(c.Length)+")", "vector("+n(c.Length)+")"), nil
	}
	return "", g.unknownType(c)
}

func (g grammar) spatialType(c *Column) (string, error) {
	sub := cmp.Or(strings.ToLower(c.Subtype), "geometry")
	srid := strconv.Itoa(c.SRID)
	switch {
	case g.engine == enginePostgres:
		kind := map[string]string{TypeGeometry: "geometry", TypeGeography: "geography"}[c.Type]
		if c.SRID > 0 {
			return kind + "(" + sub + ", " + srid + ")", nil
		}
		return kind + "(" + sub + ")", nil
	case c.SRID == 0 || !g.mysqlFamily():
		return sub, nil
	case g.engine == engineMariaDB:
		return sub + " ref_system_id=" + srid, nil
	}
	return sub + " srid " + srid, nil
}

func (g grammar) literals(vs []string) string {
	q := make([]string, len(vs))
	for i, v := range vs {
		q[i] = g.literal(v)
	}
	return strings.Join(q, ", ")
}

func (g grammar) literal(v any) string {
	switch v := v.(type) {
	case Expr:
		return string(v)
	case string:
		if g.mysqlFamily() || g.engine == engineClickHouse {
			v = strings.ReplaceAll(v, `\`, `\\`)
		}
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case bool:
		switch g.engine {
		case enginePostgres, engineDuckDB, engineClickHouse:
			return strings.ToUpper(strconv.FormatBool(v))
		}
		return map[bool]string{true: "1", false: "0"}[v]
	case time.Time:
		return "'" + v.UTC().Format("2006-01-02 15:04:05") + "'"
	case nil:
		return "NULL"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return g.literal(fmt.Sprint(v))
}

// identifierLimit is the longest name the engine keeps intact, in bytes
// or characters; zero is no limit. Postgres truncates a longer name
// (silently, so two long index names can collide); MySQL and MariaDB
// refuse one.
func (g grammar) identifierLimit() (limit int, unit string) {
	switch {
	case g.engine == enginePostgres:
		return 63, "bytes"
	case g.mysqlFamily():
		return 64, "characters"
	}
	return 0, ""
}

// checkIdentifiers refuses names longer than the engine keeps.
func (g grammar) checkIdentifiers(table string, names ...string) error {
	limit, unit := g.identifierLimit()
	if limit == 0 {
		return nil
	}
	for _, n := range names {
		size := len(n)
		if unit == "characters" {
			size = utf8.RuneCountInString(n)
		}
		if size > limit {
			return g.unsupported(table, "use the name "+strconv.Quote(n),
				fmt.Sprintf("names are at most %d %s; give it a shorter one (an index or foreign key takes one with Name or Named)", limit, unit))
		}
	}
	return nil
}

// checkBlueprintNames applies checkIdentifiers to every name a blueprint
// writes: the table, its columns, indexes, foreign keys and renames.
func (g grammar) checkBlueprintNames(bp *Blueprint) error {
	names := []string{bp.table}
	for _, c := range bp.columns {
		names = append(names, c.Name)
	}
	for _, cmd := range bp.commands {
		names = append(names, cmd.columns...)
		names = append(names, cmd.from, cmd.to)
		if cmd.index != nil {
			names = append(names, cmd.index.IndexName)
			names = append(names, cmd.index.Columns...)
		}
		if f := cmd.foreign; f != nil {
			names = append(names, f.Name, f.RefTable)
			names = append(names, f.Columns...)
			names = append(names, f.RefColumns...)
		}
	}
	return g.checkIdentifiers(bp.table, names...)
}

// namePattern is a charset, collation or engine name: written into DDL as
// is on some engines, so it may hold nothing that could end the clause.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)

// checkNames refuses charset, collation or engine names that are not
// plain names.
func (g grammar) checkNames(table, what string, names ...string) error {
	for _, n := range names {
		if n != "" && !namePattern.MatchString(n) {
			return g.unsupported(table, "name "+strconv.Quote(n)+" for "+what, "charset, collation and engine names are plain names")
		}
	}
	return nil
}

// defaultValue renders a column's default, spelling UseCurrent the
// engine's way.
func (g grammar) defaultValue(c *Column) string {
	if _, ok := c.DefaultValue.(currentTimestamp); !ok {
		return g.literal(c.DefaultValue)
	}
	return g.currentTime(c)
}

// currentTime is the engine's current time at c's precision: MySQL needs
// CURRENT_TIMESTAMP(p) for a column with fractional seconds.
func (g grammar) currentTime(c *Column) string {
	switch {
	case g.engine == engineClickHouse && c.Precision > 0:
		return "now64(" + strconv.Itoa(c.Precision) + ")"
	case g.engine == engineClickHouse && (c.Type == TypeTimestamp || c.Type == TypeTimestampTz):
		return "now64(3)"
	case g.engine == engineClickHouse:
		return "now()"
	case g.mysqlFamily() && c.Precision > 0:
		return "CURRENT_TIMESTAMP(" + strconv.Itoa(c.Precision) + ")"
	}
	return "CURRENT_TIMESTAMP"
}

// checkType rejects column definitions no engine can store.
func checkType(c *Column) error {
	if (c.Type == TypeEnum || c.Type == TypeSet) && len(c.Allowed) == 0 {
		return fmt.Errorf("schema: %s column %s.%s has no allowed values", c.Type, c.bp.table, c.Name)
	}
	if isTime(c.Type) && (c.Precision < 0 || c.Precision > 6) {
		return fmt.Errorf("schema: %s column %s.%s has precision %d; fractional seconds go from 0 to 6 digits", c.Type, c.bp.table, c.Name, c.Precision)
	}
	return nil
}

func isTime(t string) bool {
	switch t {
	case TypeTime, TypeTimeTz, TypeDateTime, TypeDateTimeTz, TypeTimestamp, TypeTimestampTz:
		return true
	}
	return false
}

func checkName(table, column string) string { return table + "_" + column + "_check" }

func (g grammar) enumCheck(table string, c *Column) string {
	return "CONSTRAINT " + g.quote(checkName(table, c.Name)) + " CHECK (" + g.quote(c.Name) + " IN (" + g.literals(c.Allowed) + "))"
}

// column renders a full column definition.
func (g grammar) column(c *Column) (string, error) { return g.columnDef(c, true) }

// columnDef renders a column definition, without NULL / NOT NULL when
// nullability is false (DuckDB adds columns unconstrained).
func (g grammar) columnDef(c *Column, nullability bool) (string, error) {
	if g.engine == engineClickHouse {
		return g.clickhouseColumn(c)
	}
	table := c.bp.table
	typ, err := g.typeOf(c)
	if err != nil {
		return "", err
	}
	if err := g.checkNames(table, "column "+c.Name, c.CharsetName, c.CollationName); err != nil {
		return "", err
	}
	if c.CharsetName != "" && !g.mysqlFamily() {
		return "", g.unsupported(table, "set the character set of column "+c.Name, "column character sets are a MySQL feature")
	}
	if c.OnUpdateNow && !g.mysqlFamily() {
		return "", g.unsupported(table, "update column "+c.Name+" on every write", "ON UPDATE CURRENT_TIMESTAMP is a MySQL feature")
	}
	if c.Identity != "" && g.engine != enginePostgres {
		return "", g.unsupported(table, "make column "+c.Name+" an identity column", "identity columns need Postgres; use an auto-incrementing column")
	}
	if (c.IsFirst || c.AfterColumn != "") && !g.mysqlFamily() {
		return "", g.unsupported(table, "position column "+c.Name, "placing a column with First or After is a MySQL and MariaDB feature")
	}
	if c.IsInvisible && !g.mysqlFamily() {
		return "", g.unsupported(table, "make column "+c.Name+" invisible", "invisible columns are a MySQL and MariaDB feature")
	}
	if c.IsInvisible && g.engine == engineMariaDB && !c.IsNullable && !c.HasDefault {
		return "", g.unsupported(table, "make column "+c.Name+" invisible", "MariaDB needs an invisible column to be nullable or have a default")
	}
	if c.VirtualExpr != "" && g.engine == enginePostgres {
		return "", g.unsupported(table, "create virtual generated column "+c.Name, "Postgres stores generated columns; use StoredAs")
	}
	var b strings.Builder
	w := func(s ...string) {
		for _, p := range s {
			b.WriteString(p)
		}
	}
	w(g.quote(c.Name), " ", typ)
	switch g.engine {
	case engineMySQL, engineMariaDB:
		if c.IsUnsigned && isNumeric(c.Type) {
			w(" unsigned")
		}
		if c.CharsetName != "" {
			w(" CHARACTER SET ", c.CharsetName)
		}
		if c.CollationName != "" {
			w(" COLLATE ", g.literal(c.CollationName))
		}
	case enginePostgres:
		if c.CollationName != "" {
			w(" COLLATE ", g.quote(c.CollationName))
		}
	case engineSQLite:
		if c.IsAutoIncrement {
			w(" PRIMARY KEY AUTOINCREMENT")
		}
		if c.CollationName != "" {
			w(" COLLATE ", c.CollationName)
		}
	case engineDuckDB:
		if c.CollationName != "" {
			w(" COLLATE ", c.CollationName)
		}
	}
	generated := c.StoredExpr != "" || c.VirtualExpr != ""
	if generated {
		expr, kind := c.StoredExpr, "STORED"
		if expr == "" {
			expr, kind = c.VirtualExpr, "VIRTUAL"
		}
		if kind == "STORED" && g.engine == engineDuckDB {
			return "", g.unsupported(table, "create stored generated column "+c.Name, "DuckDB only has virtual generated columns; use VirtualAs")
		}
		w(g.pick(" GENERATED ALWAYS AS (", " GENERATED ALWAYS AS (", " AS ("), expr, ") ", kind)
	} else if nullability {
		w(map[bool]string{true: " NULL", false: " NOT NULL"}[c.IsNullable])
	}
	if c.IsInvisible && g.mysqlFamily() {
		w(" INVISIBLE")
	}
	if c.HasDefault {
		w(" DEFAULT ", g.defaultValue(c))
	}
	if c.OnUpdateNow {
		w(" ON UPDATE ", g.currentTime(c))
	}
	if c.Type == TypeEnum && (g.engine == enginePostgres || g.engine == engineSQLite) {
		w(" ", g.enumCheck(table, c))
	}
	if g.engine == enginePostgres && identity(c) {
		w(" GENERATED ", cmp.Or(c.Identity, "BY DEFAULT"), " AS IDENTITY")
		if c.StartingValue != 0 {
			w(" (START WITH ", strconv.Itoa(c.StartingValue), ")")
		}
	}
	if c.IsAutoIncrement && !c.Changed {
		switch g.engine {
		case enginePostgres:
			w(" PRIMARY KEY")
		case engineMySQL, engineMariaDB:
			w(" AUTO_INCREMENT PRIMARY KEY")
		case engineDuckDB:
			w(" PRIMARY KEY DEFAULT nextval(", g.literal(g.quote(sequenceName(table, c.Name))), ")")
		}
	} else if c.IsAutoIncrement && g.mysqlFamily() {
		w(" AUTO_INCREMENT")
	}
	if g.mysqlFamily() {
		if c.CommentText != "" {
			w(" COMMENT ", g.literal(c.CommentText))
		}
		if c.IsFirst {
			w(" FIRST")
		} else if c.AfterColumn != "" {
			w(" AFTER ", g.quote(c.AfterColumn))
		}
	}
	return b.String(), nil
}

func sequenceName(table, column string) string { return table + "_" + column + "_seq" }

func isNumeric(t string) bool {
	switch t {
	case TypeTinyInteger, TypeSmallInteger, TypeMediumInteger, TypeInteger, TypeBigInteger, TypeDecimal, TypeFloat, TypeDouble:
		return true
	}
	return false
}

// checkTableOptions refuses table options the engine has no use for, and
// options that only apply when the table is created.
func (g grammar) checkTableOptions(bp *Blueprint, creating bool) error {
	names := []string{bp.charset, bp.collation}
	if g.mysqlFamily() {
		names = append(names, bp.engine)
	}
	if err := g.checkNames(bp.table, "the table", names...); err != nil {
		return err
	}
	switch {
	case bp.engine != "" && !creating:
		return g.unsupported(bp.table, "change the table engine", "table options apply only when creating the table")
	case bp.engine != "" && !g.mysqlFamily() && g.engine != engineClickHouse:
		return g.unsupported(bp.table, "set table engine "+bp.engine, "table engines exist on MySQL, MariaDB and ClickHouse")
	case (bp.charset != "" || bp.collation != "") && !creating:
		return g.unsupported(bp.table, "change the table character set or collation", "table options apply only when creating the table")
	case (bp.charset != "" || bp.collation != "") && !g.mysqlFamily():
		return g.unsupported(bp.table, "set the table character set or collation", "table character sets and collations are a MySQL feature")
	case bp.sorting.declared() && g.engine != engineClickHouse:
		return g.unsupported(bp.table, "set a sorting key, partition key, TTL or settings", "these are ClickHouse MergeTree table options")
	case bp.sorting.declared() && !creating:
		return g.unsupported(bp.table, "change the sorting key, partition key, TTL or settings", "table options apply only when creating the table")
	}
	return nil
}

func (g grammar) compileCreate(bp *Blueprint) ([]string, error) {
	if err := g.checkBlueprintNames(bp); err != nil {
		return nil, err
	}
	if err := bp.err(); err != nil {
		return nil, err
	}
	if err := g.checkTableOptions(bp, true); err != nil {
		return nil, err
	}
	var defs, after []string
	for _, c := range bp.columns {
		if c.Changed {
			return nil, fmt.Errorf("schema: cannot change column %s while creating table %s", c.Name, bp.table)
		}
		def, err := g.column(c)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	var primary []string
	for _, cmd := range bp.commands {
		switch {
		case cmd.kind == cmdPrimary:
			if primary != nil {
				return nil, fmt.Errorf("schema: table %s declares two primary keys", bp.table)
			}
			primary = cmd.columns
			switch g.engine {
			case engineClickHouse:
			case engineSQLite:
				defs = append(defs, "PRIMARY KEY ("+g.list(cmd.columns)+")")
			default:
				defs = append(defs, "CONSTRAINT "+g.quote(g.primaryName(bp.table, cmd.index))+" PRIMARY KEY ("+g.list(cmd.columns)+")")
			}
		case cmd.kind == cmdForeign:
			def, err := g.foreign(bp.table, cmd.foreign)
			if err != nil {
				return nil, err
			}
			defs = append(defs, def)
		case cmd.kind.createsIndex() && (g.mysqlFamily() || g.engine == engineClickHouse):
			def, err := g.inlineIndex(bp.table, cmd)
			if err != nil {
				return nil, err
			}
			defs = append(defs, def)
		case cmd.kind.createsIndex():
			s, err := g.createIndex(bp.table, cmd, bp.ifNot)
			if err != nil {
				return nil, err
			}
			after = append(after, s)
		default:
			return nil, fmt.Errorf("schema: cannot %s while creating table %s", cmd.kind, bp.table)
		}
	}
	for _, c := range bp.columns {
		if c.IsFirst || c.AfterColumn != "" {
			return nil, g.unsupported(bp.table, "position column "+c.Name, "a new table's columns are in declaration order; First and After place columns added later")
		}
	}
	if g.engine != engineClickHouse {
		for _, c := range bp.columns {
			if c.IsAutoIncrement && primary != nil && !slices.Equal(primary, []string{c.Name}) {
				return nil, g.unsupported(bp.table, "add the auto-increment column "+c.Name+" beside the primary key ("+strings.Join(primary, ", ")+")",
					"an auto-increment column is the table's primary key")
			}
		}
	}
	var head strings.Builder
	head.WriteString("CREATE ")
	if bp.temporary {
		head.WriteString("TEMPORARY ")
	}
	head.WriteString("TABLE ")
	if bp.ifNot {
		head.WriteString("IF NOT EXISTS ")
	}
	head.WriteString(g.quote(bp.table) + " (" + strings.Join(defs, ", ") + ")")
	switch g.engine {
	case engineMySQL, engineMariaDB:
		if bp.engine != "" {
			head.WriteString(" ENGINE = " + bp.engine)
		}
		if bp.charset != "" {
			head.WriteString(" DEFAULT CHARACTER SET = " + bp.charset)
		}
		if bp.collation != "" {
			head.WriteString(" COLLATE = " + g.literal(bp.collation))
		}
		for _, c := range bp.columns {
			if c.IsAutoIncrement && c.StartingValue != 0 {
				head.WriteString(" AUTO_INCREMENT = " + strconv.Itoa(c.StartingValue))
			}
		}
		if bp.comment != "" {
			head.WriteString(" COMMENT = " + g.literal(bp.comment))
		}
	case engineClickHouse:
		storage, err := g.clickhouseStorage(bp, primary)
		if err != nil {
			return nil, err
		}
		head.WriteString(storage)
	}
	var stmts []string
	if g.engine == engineDuckDB {
		stmts = g.duckdbSequences(bp)
	}
	stmts = append(stmts, head.String())
	stmts = append(stmts, after...)
	if g.engine == engineSQLite {
		stmts = append(stmts, g.sqliteStartingValues(bp)...)
	}
	return append(stmts, g.comments(bp)...), nil
}

func (g grammar) primaryName(table string, idx *Index) string {
	if g.engine == enginePostgres && (idx == nil || idx.IndexName == indexName(table, "primary", idx.Columns)) {
		return table + "_pkey"
	}
	return idx.IndexName
}

// sqliteStartingValues seeds sqlite_sequence for From(n), once: running
// CreateIfNotExists again leaves an existing sequence alone.
func (g grammar) sqliteStartingValues(bp *Blueprint) []string {
	var stmts []string
	for _, c := range bp.columns {
		if c.IsAutoIncrement && c.StartingValue != 0 {
			name := g.literal(bp.table)
			stmts = append(stmts, "INSERT INTO sqlite_sequence (name, seq) SELECT "+name+", "+strconv.Itoa(c.StartingValue-1)+
				" WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = "+name+")")
		}
	}
	return stmts
}

// duckdbSequences creates the sequences DuckDB auto-increment columns draw
// from; DuckDB has no serial types.
func (g grammar) duckdbSequences(bp *Blueprint) []string {
	var stmts []string
	for _, c := range bp.columns {
		if !c.IsAutoIncrement {
			continue
		}
		s := "CREATE SEQUENCE IF NOT EXISTS " + g.quote(sequenceName(bp.table, c.Name))
		if c.StartingValue != 0 {
			s += " START WITH " + strconv.Itoa(c.StartingValue)
		}
		stmts = append(stmts, s)
	}
	return stmts
}

// compileAlter compiles Schema::table(). On SQLite it returns errRebuild
// when an operation needs the table to be rebuilt.
func (g grammar) compileAlter(bp *Blueprint) ([]string, error) {
	drops, rest, err := g.compileAlterPhases(bp)
	return append(drops, rest...), err
}

// compileAlterPhases compiles an alteration as the index and constraint
// drops, then everything else.
func (g grammar) compileAlterPhases(bp *Blueprint) (drops, stmts []string, err error) {
	if err := g.checkBlueprintNames(bp); err != nil {
		return nil, nil, err
	}
	if err := bp.err(); err != nil {
		return nil, nil, err
	}
	if err := g.checkTableOptions(bp, false); err != nil {
		return nil, nil, err
	}
	dropCmds, rest := splitDrops(bp.commands)
	for _, cmd := range dropCmds {
		s, err := g.alterCommand(bp.table, cmd)
		if err != nil {
			return nil, nil, err
		}
		drops = append(drops, s...)
	}
	for _, c := range bp.columns {
		alter := g.addColumn
		if c.Changed {
			alter = g.changeColumn
		}
		s, err := alter(c)
		if err != nil {
			return nil, nil, err
		}
		stmts = append(stmts, s...)
	}
	for _, cmd := range rest {
		s, err := g.alterCommand(bp.table, cmd)
		if err != nil {
			return nil, nil, err
		}
		stmts = append(stmts, s...)
	}
	t := g.quote(bp.table)
	if bp.comment != "" {
		switch g.engine {
		case engineMySQL, engineMariaDB:
			stmts = append(stmts, "ALTER TABLE "+t+" COMMENT = "+g.literal(bp.comment))
		case engineClickHouse:
			stmts = append(stmts, "ALTER TABLE "+t+" MODIFY COMMENT "+g.literal(bp.comment))
		}
	}
	if g.mysqlFamily() {
		for _, c := range bp.columns {
			if c.IsAutoIncrement && c.StartingValue != 0 {
				stmts = append(stmts, "ALTER TABLE "+t+" AUTO_INCREMENT = "+strconv.Itoa(c.StartingValue))
			}
		}
	}
	return drops, append(stmts, g.comments(bp)...), nil
}

// splitDrops separates an alteration's index, foreign key and primary key
// drops, which run before any column changes: an index or constraint must
// go before the columns it covers change or go (DuckDB refuses to drop any
// column an index after it depends on, MySQL to modify a constrained
// column).
func splitDrops(cmds []*command) (drops, rest []*command) {
	for _, cmd := range cmds {
		switch cmd.kind {
		case cmdDropIndex, cmdDropForeign, cmdDropPrimary:
			drops = append(drops, cmd)
		default:
			rest = append(rest, cmd)
		}
	}
	return drops, rest
}

func (g grammar) addColumn(c *Column) ([]string, error) {
	t := g.quote(c.bp.table)
	if g.engine != engineDuckDB {
		def, err := g.column(c)
		return []string{"ALTER TABLE " + t + " ADD COLUMN " + def}, err
	}
	if c.IsAutoIncrement {
		return nil, g.unsupported(c.bp.table, "add auto-incrementing column "+c.Name, "DuckDB cannot add a primary key column to an existing table")
	}
	def, err := g.columnDef(c, false)
	if err != nil {
		return nil, err
	}
	stmts := []string{"ALTER TABLE " + t + " ADD COLUMN " + def}
	if !c.IsNullable && c.StoredExpr == "" && c.VirtualExpr == "" {
		stmts = append(stmts, "ALTER TABLE "+t+" ALTER COLUMN "+g.quote(c.Name)+" SET NOT NULL")
	}
	return stmts, nil
}

func (g grammar) changeColumn(c *Column) ([]string, error) {
	table := c.bp.table
	t := g.quote(table)
	switch g.engine {
	case engineSQLite:
		return nil, errRebuild
	case engineMySQL, engineMariaDB:
		def, err := g.column(c)
		return []string{"ALTER TABLE " + t + " MODIFY " + def}, err
	case engineClickHouse:
		def, err := g.column(c)
		return []string{"ALTER TABLE " + t + " MODIFY COLUMN " + def}, err
	}
	if identity(c) {
		return nil, g.unsupported(table, "change column "+c.Name+" into an identity column or restart it", "identity changes need raw SQL (ALTER COLUMN ... ADD GENERATED / RESTART)")
	}
	if c.StoredExpr != "" || c.VirtualExpr != "" {
		return nil, g.unsupported(table, "change generated column "+c.Name, "drop and re-add the column")
	}
	typ, err := g.typeOf(c)
	if err != nil {
		return nil, err
	}
	col := "ALTER TABLE " + t + " ALTER COLUMN " + g.quote(c.Name)
	var stmts []string
	switch g.engine {
	case enginePostgres:
		if c.Type == TypeEnum {
			stmts = append(stmts, "ALTER TABLE "+t+" DROP CONSTRAINT IF EXISTS "+g.quote(checkName(table, c.Name)))
		}
		using := col + " TYPE " + typ
		if c.CollationName != "" {
			using += " COLLATE " + g.quote(c.CollationName)
		}
		stmts = append(stmts, using+" USING "+g.quote(c.Name)+"::"+typ)
	case engineDuckDB:
		if c.CollationName != "" {
			typ += " COLLATE " + c.CollationName
		}
		stmts = append(stmts, col+" TYPE "+typ)
	}
	stmts = append(stmts, col+map[bool]string{true: " DROP NOT NULL", false: " SET NOT NULL"}[c.IsNullable])
	switch {
	case c.IsAutoIncrement:
	case c.HasDefault:
		stmts = append(stmts, col+" SET DEFAULT "+g.defaultValue(c))
	default:
		stmts = append(stmts, col+" DROP DEFAULT")
	}
	if c.Type == TypeEnum && g.engine == enginePostgres {
		stmts = append(stmts, "ALTER TABLE "+t+" ADD "+g.enumCheck(table, c))
	}
	return stmts, nil
}

func (g grammar) alterCommand(table string, cmd *command) ([]string, error) {
	t := g.quote(table)
	one := func(s string) ([]string, error) { return []string{s}, nil }
	switch cmd.kind {
	case cmdUnique, cmdIndex, cmdFullText, cmdSpatial, cmdHnsw:
		s, err := g.createIndex(table, cmd, false)
		if err != nil {
			return nil, err
		}
		return one(s)
	case cmdDropColumn:
		stmts := make([]string, len(cmd.columns))
		for i, c := range cmd.columns {
			stmts[i] = "ALTER TABLE " + t + " DROP COLUMN " + g.quote(c)
		}
		return stmts, nil
	case cmdRenameColumn:
		return one("ALTER TABLE " + t + " RENAME COLUMN " + g.quote(cmd.from) + " TO " + g.quote(cmd.to))
	case cmdRenameIndex:
		switch g.engine {
		case engineMySQL, engineMariaDB:
			return one("ALTER TABLE " + t + " RENAME INDEX " + g.quote(cmd.from) + " TO " + g.quote(cmd.to))
		case enginePostgres:
			return one("ALTER INDEX " + g.quote(cmd.from) + " RENAME TO " + g.quote(cmd.to))
		case engineSQLite:
			return nil, errRebuild
		}
		return nil, g.unsupported(table, "rename index "+cmd.from, "drop it and create it under the new name")
	case cmdDropIndex:
		if g.mysqlFamily() || g.engine == engineClickHouse {
			return one("ALTER TABLE " + t + " DROP INDEX " + g.quote(cmd.index.IndexName))
		}
		return one("DROP INDEX " + g.quote(cmd.index.IndexName))
	case cmdForeign:
		switch g.engine {
		case engineSQLite:
			return nil, errRebuild
		case engineDuckDB:
			return nil, g.unsupported(table, "add foreign key "+cmd.foreign.Name+" to an existing table", "DuckDB declares foreign keys only in CREATE TABLE")
		}
		def, err := g.foreign(table, cmd.foreign)
		if err != nil {
			return nil, err
		}
		return one("ALTER TABLE " + t + " ADD " + def)
	case cmdDropForeign:
		switch g.engine {
		case engineSQLite:
			return nil, errRebuild
		case engineMySQL, engineMariaDB:
			return one("ALTER TABLE " + t + " DROP FOREIGN KEY " + g.quote(cmd.index.IndexName))
		case enginePostgres:
			return one("ALTER TABLE " + t + " DROP CONSTRAINT " + g.quote(cmd.index.IndexName))
		}
		return nil, g.unsupported(table, "drop foreign key "+cmd.index.IndexName, g.noForeignKeyChanges())
	case cmdPrimary:
		switch g.engine {
		case engineSQLite:
			return nil, errRebuild
		case engineClickHouse:
			return nil, g.unsupported(table, "add a primary key to an existing table", "a MergeTree primary key is fixed when the table is created")
		}
		return one("ALTER TABLE " + t + " ADD CONSTRAINT " + g.quote(g.primaryName(table, cmd.index)) + " PRIMARY KEY (" + g.list(cmd.columns) + ")")
	case cmdDropPrimary:
		switch g.engine {
		case engineSQLite:
			return nil, errRebuild
		case engineMySQL, engineMariaDB:
			return one("ALTER TABLE " + t + " DROP PRIMARY KEY")
		case enginePostgres:
			return one("ALTER TABLE " + t + " DROP CONSTRAINT " + g.quote(table+"_pkey"))
		}
		return nil, g.unsupported(table, "drop the primary key", "the primary key is fixed when the table is created")
	}
	return nil, fmt.Errorf("schema: unknown command %d on table %s", cmd.kind, table)
}

func (g grammar) noForeignKeyChanges() string {
	if g.engine == engineClickHouse {
		return "ClickHouse has no foreign keys"
	}
	return "DuckDB cannot drop constraints"
}

// duckdbForeignActions are the referential actions DuckDB enforces.
var duckdbForeignActions = []string{"", "no action", "restrict"}

func (g grammar) foreign(table string, f *Foreign) (string, error) {
	op := "add foreign key " + f.Name
	switch {
	case g.engine == engineClickHouse:
		return "", g.unsupported(table, op, "ClickHouse has no foreign keys")
	case g.engine == engineDuckDB && (!slices.Contains(duckdbForeignActions, f.OnDel) || !slices.Contains(duckdbForeignActions, f.OnUpd)):
		return "", g.unsupported(table, op+" with ON DELETE "+cmp.Or(f.OnDel, "no action")+" ON UPDATE "+cmp.Or(f.OnUpd, "no action"),
			"DuckDB foreign keys support only NO ACTION and RESTRICT")
	case f.Defer && g.engine != enginePostgres:
		return "", g.unsupported(table, op+" as deferrable", "deferrable constraints need Postgres")
	case f.RefTable == "":
		return "", fmt.Errorf("schema: foreign key %s on table %s references no table; call On", f.Name, table)
	}
	refCols := f.RefColumns
	if len(refCols) == 0 {
		refCols = []string{"id"}
	}
	s := "CONSTRAINT " + g.quote(f.Name) + " FOREIGN KEY (" + g.list(f.Columns) + ") REFERENCES " +
		g.quote(f.RefTable) + " (" + g.list(refCols) + ")"
	if f.OnDel != "" {
		s += " ON DELETE " + strings.ToUpper(f.OnDel)
	}
	if f.OnUpd != "" {
		s += " ON UPDATE " + strings.ToUpper(f.OnUpd)
	}
	if f.Defer {
		s += " DEFERRABLE"
		if f.InitDefer {
			s += " INITIALLY DEFERRED"
		}
	}
	return s, nil
}

// inlineIndex renders an index inside CREATE TABLE, which keeps MySQL and
// ClickHouse table creation a single statement (and CreateIfNotExists
// idempotent).
func (g grammar) inlineIndex(table string, cmd *command) (string, error) {
	if err := g.checkIndexForm(table, cmd); err != nil {
		return "", err
	}
	if g.engine == engineClickHouse {
		return g.clickhouseIndex(table, cmd)
	}
	name, cols := g.quote(cmd.index.IndexName), g.indexTarget(cmd)
	switch cmd.kind {
	case cmdUnique:
		return "UNIQUE KEY " + name + cols + g.mysqlAlgorithm(cmd.index), nil
	case cmdIndex:
		return "INDEX " + name + cols + g.mysqlAlgorithm(cmd.index), nil
	case cmdFullText:
		return "FULLTEXT INDEX " + name + cols, nil
	case cmdSpatial:
		return "SPATIAL INDEX " + name + cols, nil
	case cmdHnsw:
		distance, err := g.mariadbDistance(table, cmd)
		if err != nil {
			return "", err
		}
		return "VECTOR INDEX " + name + cols + " DISTANCE=" + distance, nil
	}
	return "", g.unsupported(table, cmd.kind.String()+" "+cmd.index.IndexName, "this index kind has no inline form here")
}

// mariadbDistance is the DISTANCE option of a MariaDB vector index for the
// metric an HnswIndex declares; MariaDB has no inner-product distance.
func (g grammar) mariadbDistance(table string, cmd *command) (string, error) {
	op := cmd.kind.String() + " " + cmd.index.IndexName
	if g.engine != engineMariaDB {
		return "", g.unsupported(table, op, "vector indexes need pgvector or MariaDB 11.7+")
	}
	distance, ok := map[string]string{"vector_l2_ops": "euclidean", "vector_cosine_ops": "cosine", "": "euclidean"}[cmd.index.VectorOp]
	if !ok {
		return "", g.unsupported(table, op, "MariaDB vector indexes measure euclidean or cosine distance, not inner product")
	}
	return distance, nil
}

func (g grammar) mysqlAlgorithm(idx *Index) string {
	if idx.Algo == "" {
		return ""
	}
	return " USING " + strings.ToUpper(idx.Algo)
}

func (g grammar) createIndex(table string, cmd *command, ifNot bool) (string, error) {
	idx := cmd.index
	if g.engine == engineClickHouse {
		def, err := g.clickhouseIndex(table, cmd)
		return "ALTER TABLE " + g.quote(table) + " ADD " + def, err
	}
	if err := g.checkIndexForm(table, cmd); err != nil {
		return "", err
	}
	exists := ""
	if ifNot && !g.mysqlFamily() {
		exists = "IF NOT EXISTS "
	}
	name, on := exists+g.quote(idx.IndexName), " ON "+g.quote(table)
	cols := g.indexTarget(cmd)
	op := cmd.kind.String() + " " + idx.IndexName
	switch cmd.kind {
	case cmdFullText:
		switch g.engine {
		case engineMySQL, engineMariaDB:
			return "CREATE FULLTEXT INDEX " + name + on + cols, nil
		case enginePostgres:
			lang := cmp.Or(idx.Lang, "english")
			parts := make([]string, len(cmd.columns))
			for i, c := range cmd.columns {
				parts[i] = "to_tsvector(" + g.literal(lang) + ", " + g.quote(c) + ")"
			}
			return "CREATE INDEX " + name + on + " USING gin ((" + strings.Join(parts, " || ") + "))", nil
		}
		return "", g.unsupported(table, op, "full text indexes need Postgres, MySQL or MariaDB")
	case cmdSpatial:
		switch g.engine {
		case engineMySQL, engineMariaDB:
			return "CREATE SPATIAL INDEX " + name + on + cols, nil
		case enginePostgres:
			return "CREATE INDEX " + name + on + " USING gist" + cols, nil
		}
		return "", g.unsupported(table, op, "spatial indexes need Postgres, MySQL or MariaDB")
	case cmdHnsw:
		if g.engine == engineMariaDB {
			distance, err := g.mariadbDistance(table, cmd)
			if err != nil {
				return "", err
			}
			return "CREATE VECTOR INDEX " + name + on + cols + " DISTANCE=" + distance, nil
		}
		if g.engine != enginePostgres {
			return "", g.unsupported(table, op, "vector indexes need pgvector or MariaDB 11.7+")
		}
		return "CREATE INDEX " + name + on + " USING hnsw (" + g.quote(cmd.columns[0]) + " " + cmp.Or(idx.VectorOp, "vector_l2_ops") + ")", nil
	}
	unique := map[bool]string{true: "UNIQUE ", false: ""}[cmd.kind == cmdUnique]
	s := "CREATE " + unique + "INDEX " + name + on
	where := ""
	if idx.Predicate != "" {
		where = " WHERE " + idx.Predicate
	}
	switch {
	case idx.Algo == "":
		return s + cols + where, nil
	case g.engine == enginePostgres:
		return s + " USING " + idx.Algo + cols + where, nil
	case g.engine == engineDuckDB:
		return "", g.unsupported(table, op+" using "+idx.Algo, "DuckDB picks its index structure itself")
	case g.mysqlFamily():
		return s + cols + g.mysqlAlgorithm(idx), nil
	case strings.EqualFold(idx.Algo, "btree"):
		return s + cols, nil
	}
	return "", g.unsupported(table, op+" using "+idx.Algo, "SQLite indexes are always B-trees")
}

// indexTarget is the parenthesised column list, or the expression of a
// RawIndex.
func (g grammar) indexTarget(cmd *command) string {
	if cmd.index.Expression != "" {
		return " ((" + cmd.index.Expression + "))"
	}
	return " (" + g.list(cmd.columns) + ")"
}

// checkIndexForm refuses partial and expression indexes where the engine
// has none.
func (g grammar) checkIndexForm(table string, cmd *command) error {
	op := cmd.kind.String() + " " + cmd.index.IndexName
	if cmd.index.Predicate != "" {
		if g.engine != enginePostgres && g.engine != engineSQLite {
			return g.unsupported(table, op+" over part of the rows", "partial indexes need Postgres or SQLite")
		}
		if cmd.kind != cmdIndex && cmd.kind != cmdUnique {
			return g.unsupported(table, op+" over part of the rows", "only plain and unique indexes can be partial")
		}
	}
	if cmd.index.Expression != "" && g.engine == engineMariaDB {
		return g.unsupported(table, op+" on an expression", "MariaDB indexes columns only; index a generated column")
	}
	return nil
}

func (g grammar) comments(bp *Blueprint) []string {
	if g.engine != enginePostgres && g.engine != engineDuckDB {
		return nil
	}
	var stmts []string
	if bp.comment != "" {
		stmts = append(stmts, "COMMENT ON TABLE "+g.quote(bp.table)+" IS "+g.literal(bp.comment))
	}
	for _, c := range bp.columns {
		if c.CommentText != "" {
			stmts = append(stmts, "COMMENT ON COLUMN "+g.quote(bp.table)+"."+g.quote(c.Name)+" IS "+g.literal(c.CommentText))
		}
	}
	return stmts
}

func (g grammar) drop(table string, ifExists bool) string {
	s := "DROP TABLE "
	if ifExists {
		s += "IF EXISTS "
	}
	return s + g.quote(table)
}

func (g grammar) rename(from, to string) string {
	if g.mysqlFamily() || g.engine == engineClickHouse {
		return "RENAME TABLE " + g.quote(from) + " TO " + g.quote(to)
	}
	return "ALTER TABLE " + g.quote(from) + " RENAME TO " + g.quote(to)
}

// foreignKeyChecks is the session statement toggling constraint
// enforcement on SQLite and MySQL.
func (g grammar) foreignKeyChecks(on bool) string {
	if g.engine == engineSQLite {
		return "PRAGMA foreign_keys = " + map[bool]string{true: "ON", false: "OFF"}[on]
	}
	return "SET FOREIGN_KEY_CHECKS = " + map[bool]string{true: "1", false: "0"}[on]
}

func (g grammar) unknownType(c *Column) error {
	return fmt.Errorf("schema: unknown column type %q for %s.%s", c.Type, c.bp.table, c.Name)
}
