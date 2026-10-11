package main

import (
	"cmp"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

const (
	ormImport = "github.com/stubbedev/storm/orm"
	directive = "orm:table"
)

type model struct {
	Struct, Table, Schema, Var  string
	SchemaType, Query           string
	Connection, Key, KeyGen     string
	KeyType                     string
	CreatedAt, UpdatedAt        string
	DeletedAt, Morph, ModelPath string
	TimePrecision               int
	Fields                      []field

	pos     token.Position
	typ     *types.Struct
	opts    options
	keyType types.Type
}

type field struct {
	Name, Path, Column, Type string
	PK, Virtual, NullZero    bool

	typ types.Type
}

type options struct {
	table, key, keyType, connection, morph, schema string
	timePrecision                                  int
	timestamps, softDeletes                        bool
}

// modelError locates a model's error at its declaration.
func modelError(pos token.Position, name string, format string, args ...any) error {
	return fmt.Errorf("ormgen: %s: %s: %s", pos, name, fmt.Sprintf(format, args...))
}

// models reads every //orm:table struct of the package into a model,
// validated against each other and against the package's own names.
func models(l loaded, imports *imports) ([]model, error) {
	var ms []model
	for _, file := range l.files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			if text, found, err := findDirective(gen.Doc); err != nil {
				return nil, fmt.Errorf("ormgen: %s: %w", l.fset.Position(gen.Pos()), err)
			} else if found && gen.Lparen.IsValid() {
				return nil, fmt.Errorf("ormgen: %s: %q is above a grouped type declaration; put it on the struct it describes", l.fset.Position(gen.Pos()), text)
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				doc := ts.Doc
				if doc == nil && !gen.Lparen.IsValid() {
					doc = gen.Doc
				}
				text, found, err := findDirective(doc)
				if err != nil || !found {
					if err != nil {
						return nil, fmt.Errorf("ormgen: %s: %w", l.fset.Position(ts.Pos()), err)
					}
					continue
				}
				m, err := declared(l, ts, text)
				if err != nil {
					return nil, err
				}
				ms = append(ms, m)
			}
		}
	}
	if len(ms) == 0 {
		return nil, errors.New("ormgen: no //orm:table structs found")
	}
	if err := checkNames(l, ms); err != nil {
		return nil, err
	}
	imports.reserve(generatedNames(ms)...)
	members, err := tableMembers(l.pkg)
	if err != nil {
		return nil, err
	}
	for i := range ms {
		if err := ms[i].build(imports, members); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(ms, func(a, b model) int { return strings.Compare(a.Struct, b.Struct) })
	return ms, nil
}

// findDirective returns the //orm:table line of doc; a second one is an
// error rather than ignored.
func findDirective(doc *ast.CommentGroup) (string, bool, error) {
	if doc == nil {
		return "", false, nil
	}
	var found string
	for _, c := range doc.List {
		rest, ok := strings.CutPrefix(c.Text, "//"+directive)
		if !ok || rest != "" && !unicode.IsSpace(rune(rest[0])) {
			continue
		}
		if found != "" {
			return "", false, fmt.Errorf("two directives, %q and %q, on one declaration", found, c.Text)
		}
		found = c.Text
	}
	return found, found != "", nil
}

// declared builds the parts of a model its declaration fixes: the struct,
// its directive and the names it generates.
func declared(l loaded, ts *ast.TypeSpec, text string) (model, error) {
	pos := l.fset.Position(ts.Pos())
	name := ts.Name.Name
	m := model{Struct: name, pos: pos}
	if ts.TypeParams != nil {
		return m, modelError(pos, name, "a model cannot have type parameters")
	}
	if ts.Assign.IsValid() {
		return m, modelError(pos, name, "%q is on a type alias; put it on the struct", text)
	}
	obj, ok := l.pkg.Scope().Lookup(name).(*types.TypeName)
	if !ok {
		return m, modelError(pos, name, "not a type of package %s", l.pkg.Path())
	}
	if m.typ, ok = obj.Type().Underlying().(*types.Struct); !ok {
		return m, modelError(pos, name, "%q is on a %s, not a struct", text, obj.Type().Underlying())
	}
	opts, err := parseDirective(text)
	if err != nil {
		return m, modelError(pos, name, "%v", err)
	}
	m.opts = opts
	m.Table, m.Connection, m.Morph = opts.table, opts.connection, opts.morph
	m.Schema = cmp.Or(opts.schema, camel(opts.table))
	if !token.IsIdentifier(m.Schema) {
		return m, modelError(pos, name, "table %q gives the schema name %q, which is not a Go identifier; set schema=<Name>", opts.table, m.Schema)
	}
	if m.Schema == name {
		return m, modelError(pos, name, "the schema name %s is the struct's name; set schema=<Name>", m.Schema)
	}
	m.Var = strings.ToLower(m.Schema[:1]) + m.Schema[1:] + "Table"
	m.SchemaType = m.Schema + "Schema"
	m.Query = name + "Query"
	return m, nil
}

// parseDirective reads "//orm:table <name> [key=value | flag]...".
func parseDirective(text string) (options, error) {
	args := strings.Fields(strings.TrimPrefix(text, "//"+directive))
	if len(args) == 0 {
		return options{}, fmt.Errorf("%q names no table", text)
	}
	o := options{table: args[0], timestamps: true}
	if strings.Contains(o.table, "=") {
		return o, fmt.Errorf("%q names no table before its options", text)
	}
	values := map[string]*string{"key": &o.key, "key_type": &o.keyType, "connection": &o.connection, "morph": &o.morph, "schema": &o.schema}
	flags := map[string]*bool{"timestamps": &o.timestamps, "soft_deletes": &o.softDeletes}
	known := append(slices.Collect(maps.Keys(values)), slices.Collect(maps.Keys(flags))...)
	known = append(known, "time_precision")
	slices.Sort(known)
	seen := map[string]bool{}
	for _, arg := range args[1:] {
		key, val, hasVal := strings.Cut(arg, "=")
		if seen[key] {
			return o, fmt.Errorf("option %s is given twice", key)
		}
		seen[key] = true
		switch dst, flag := values[key], flags[key]; {
		case dst != nil:
			if val == "" {
				return o, fmt.Errorf("option %s needs a value: %s=<value>", key, key)
			}
			*dst = val
		case flag != nil:
			if !hasVal {
				*flag = true
				continue
			}
			b, err := strconv.ParseBool(val)
			if err != nil {
				return o, fmt.Errorf("option %s takes true or false, not %q", key, val)
			}
			*flag = b
		case key == "time_precision":
			n, err := strconv.Atoi(val)
			if err != nil || n < 0 || n > 9 {
				return o, fmt.Errorf("time_precision must be 0 to 9, got %q", val)
			}
			o.timePrecision = n
		default:
			return o, fmt.Errorf("unknown option %q (known: %s)", key, strings.Join(known, ", "))
		}
	}
	return o, nil
}

func generatedNames(ms []model) []string {
	var names []string
	for _, m := range ms {
		names = append(names, m.Schema, m.Var, m.SchemaType, m.Query)
	}
	return names
}

// checkNames refuses generated names that collide with each other or with
// what the package already declares.
func checkNames(l loaded, ms []model) error {
	owner := map[string]string{}
	for _, m := range ms {
		for _, name := range []string{m.Schema, m.Var, m.SchemaType, m.Query} {
			if other, ok := owner[name]; ok {
				return modelError(m.pos, m.Struct, "generates %s, which %s generates too; set schema=<Name> on one of them", name, other)
			}
			owner[name] = m.Struct
			if obj := l.pkg.Scope().Lookup(name); obj != nil {
				return modelError(m.pos, m.Struct, "generates %s, which the package already declares at %s", name, l.fset.Position(obj.Pos()))
			}
		}
	}
	return nil
}

// tableMembers names the embedded Table field of the generated schema
// struct and the methods it promotes from *orm.Table (Find, Where, ...),
// which a column field must not shadow. Table's fields may be shadowed:
// Countries.Name is the column, Countries.Table.Name the table's name.
func tableMembers(pkg *types.Package) (map[string]bool, error) {
	i := slices.IndexFunc(pkg.Imports(), func(p *types.Package) bool { return p.Path() == ormImport })
	if i < 0 {
		return nil, fmt.Errorf("ormgen: package %s does not import %s", pkg.Path(), ormImport)
	}
	table, ok := pkg.Imports()[i].Scope().Lookup("Table").(*types.TypeName)
	if !ok {
		return nil, fmt.Errorf("ormgen: %s declares no Table type", ormImport)
	}
	named, ok := table.Type().(*types.Named)
	if !ok {
		return nil, fmt.Errorf("ormgen: %s.Table is not a named type", ormImport)
	}
	members := map[string]bool{"Table": true}
	for m := range named.Methods() {
		members[m.Name()] = true
	}
	return members, nil
}

// build reads m's fields and settles its key, timestamps and soft deletes.
func (m *model) build(imports *imports, members map[string]bool) error {
	fail := func(format string, args ...any) error { return modelError(m.pos, m.Struct, format, args...) }
	if err := m.walk(m.typ, "", imports); err != nil {
		return err
	}
	if m.ModelPath == "" {
		return fail("must embed orm.Model")
	}
	if len(m.Fields) == 0 {
		return fail("has no db-tagged fields")
	}
	names, columns := map[string]bool{}, map[string]bool{}
	for _, f := range m.Fields {
		switch {
		case members[f.Name]:
			return fail("field %s clashes with %s of the generated %s, promoted from orm.Table; rename the field", f.Name, f.Name, m.SchemaType)
		case names[f.Name]:
			return fail("two column fields are named %s", f.Name)
		case columns[f.Column]:
			return fail("two fields are tagged db:%q", f.Column)
		}
		names[f.Name], columns[f.Column] = true, true
	}
	if err := m.settleKey(); err != nil {
		return fail("%v", err)
	}
	if m.opts.timestamps {
		m.CreatedAt, m.UpdatedAt = m.stored("created_at"), m.stored("updated_at")
	}
	if m.opts.softDeletes {
		if m.DeletedAt = m.stored("deleted_at"); m.DeletedAt == "" {
			return fail("soft_deletes needs a deleted_at column")
		}
	}
	m.TimePrecision = m.opts.timePrecision
	m.KeyType = imports.orm() + ".NoKey"
	if m.keyType != nil {
		m.KeyType = imports.typeString(m.keyType)
	}
	return nil
}

// walk collects the db-tagged fields of st, following embedded structs.
func (m *model) walk(st *types.Struct, path string, imports *imports) error {
	fail := func(format string, args ...any) error { return modelError(m.pos, m.Struct, format, args...) }
	for i := range st.NumFields() {
		f := st.Field(i)
		fieldPath := f.Name()
		if path != "" {
			fieldPath = path + "." + f.Name()
		}
		db, tagged := reflect.StructTag(st.Tag(i)).Lookup("db")
		if f.Embedded() && !tagged {
			switch t := f.Type().(type) {
			case *types.Named:
				if isORMModel(t) {
					if m.ModelPath != "" {
						return fail("embeds orm.Model twice, at %s and %s", m.ModelPath, fieldPath)
					}
					m.ModelPath = fieldPath
					continue
				}
				if inner, ok := t.Underlying().(*types.Struct); ok {
					if err := m.walk(inner, fieldPath, imports); err != nil {
						return err
					}
				}
			case *types.Pointer:
				return fail("embedded %s is a pointer, which may be nil; embed the struct to give it columns, or tag it db:\"-\"", fieldPath)
			}
			continue
		}
		if !tagged || db == "-" {
			continue
		}
		fd, err := parseTag(db)
		if err != nil {
			return fail("field %s: %v", fieldPath, err)
		}
		if strings.Contains(types.TypeString(f.Type(), nil), "invalid type") {
			return fail("field %s: its type %s does not type-check", fieldPath, types.TypeString(f.Type(), nil))
		}
		fd.Name, fd.Path, fd.typ, fd.Type = f.Name(), fieldPath, f.Type(), imports.typeString(f.Type())
		m.Fields = append(m.Fields, fd)
	}
	return nil
}

func isORMModel(t *types.Named) bool {
	obj := t.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == ormImport && obj.Name() == "Model"
}

// parseTag reads a db tag: "column[,pk][,virtual][,nullzero]".
func parseTag(db string) (field, error) {
	parts := strings.Split(db, ",")
	f := field{Column: parts[0]}
	if f.Column == "" || strings.ContainsFunc(f.Column, unicode.IsSpace) {
		return f, fmt.Errorf("db:%q needs a column name without spaces before its options", db)
	}
	options := map[string]*bool{"pk": &f.PK, "virtual": &f.Virtual, "nullzero": &f.NullZero}
	for _, raw := range parts[1:] {
		opt := strings.TrimSpace(raw)
		dst, ok := options[opt]
		switch {
		case !ok:
			return f, fmt.Errorf("db:%q has unknown option %q (known: nullzero, pk, virtual)", db, opt)
		case *dst:
			return f, fmt.Errorf("db:%q gives %s twice", db, opt)
		}
		*dst = true
	}
	if f.Virtual && (f.PK || f.NullZero) {
		return f, fmt.Errorf("db:%q: a virtual column is never written, so it can be neither pk nor nullzero", db)
	}
	return f, nil
}

// stored returns column when m stores it.
func (m *model) stored(column string) string {
	if slices.ContainsFunc(m.Fields, func(f field) bool { return f.Column == column && !f.Virtual }) {
		return column
	}
	return ""
}

// settleKey picks the primary key from key=, a pk tag or an id column, and
// how it is generated from key_type= or its Go type.
func (m *model) settleKey() error {
	var tagged []string
	for _, f := range m.Fields {
		if f.PK {
			tagged = append(tagged, f.Column)
		}
	}
	switch {
	case len(tagged) > 1:
		return fmt.Errorf("columns %s are all tagged pk; a model has one primary key", strings.Join(tagged, ", "))
	case m.opts.key != "" && len(tagged) == 1 && tagged[0] != m.opts.key:
		return fmt.Errorf("key=%s disagrees with the pk tag on %s", m.opts.key, tagged[0])
	case m.opts.key != "":
		m.Key = m.opts.key
	case len(tagged) == 1:
		m.Key = tagged[0]
	default:
		m.Key = m.stored("id")
	}
	if m.Key == "" {
		if m.opts.keyType != "" {
			return fmt.Errorf("key_type=%s needs a primary key column", m.opts.keyType)
		}
		return nil
	}
	i := slices.IndexFunc(m.Fields, func(f field) bool { return f.Column == m.Key && !f.Virtual })
	if i < 0 {
		return fmt.Errorf("primary key column %q not found", m.Key)
	}
	m.keyType = m.Fields[i].typ
	basic, _ := m.keyType.Underlying().(*types.Basic)
	integer := basic != nil && basic.Info()&types.IsInteger != 0
	switch m.opts.keyType {
	case "":
		m.KeyGen = map[bool]string{true: "KeyAutoIncrement", false: "KeyManual"}[integer]
	case "increments":
		if !integer {
			return fmt.Errorf("key_type=increments needs an integer key, and %s is %s", m.Fields[i].Path, m.keyType)
		}
		m.KeyGen = "KeyAutoIncrement"
	case "uuid":
		m.KeyGen = "KeyUUID"
	case "ulid":
		m.KeyGen = "KeyULID"
	case "manual":
		m.KeyGen = "KeyManual"
	default:
		return fmt.Errorf("unknown key_type %q (known: increments, manual, ulid, uuid)", m.opts.keyType)
	}
	return nil
}

func camel(s string) string {
	var b strings.Builder
	for part := range strings.SplitSeq(s, "_") {
		if part != "" {
			b.WriteString(strings.ToUpper(part[:1]))
			b.WriteString(part[1:])
		}
	}
	return b.String()
}
