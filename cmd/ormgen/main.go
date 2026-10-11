// Command ormgen generates typed table and column descriptors for model
// structs annotated with an orm:table directive — the Go counterpart of
// Laravel 13's #[Table], #[Connection], #[WithoutTimestamps] attributes and
// the SoftDeletes / HasUuids traits:
//
//	//orm:table users key=id connection=default soft_deletes
//	type User struct {
//		orm.Model
//		ID         int64      `db:"id"`
//		Email      string     `db:"email"`
//		CreatedAt  time.Time  `db:"created_at"`
//		UpdatedAt  time.Time  `db:"updated_at"`
//		DeletedAt  *time.Time `db:"deleted_at"`
//		PostsCount int64      `db:"posts_count,virtual"` // filled by WithCount
//		Posts      []Post     // no db tag: not a column (relations)
//	}
//
// Directive options:
//
//	key=<column>          primary key (default: field tagged `db:",pk"`, else "id")
//	key_type=<type>       increments (default for integer keys) | uuid | ulid | manual
//	connection=<name>     registered connection (default: orm.DefaultConnection)
//	timestamps=false      don't maintain created_at / updated_at
//	soft_deletes          use deleted_at for soft deletes
//	morph=<alias>         value stored in polymorphic *_type columns (default: table name)
//	time_precision=<n>    fractional second digits of maintained timestamps (default 0)
//	schema=<Name>         name of the generated schema value (default: table in CamelCase)
//
// Column tag options: pk (primary key), virtual (selected only, never
// written), nullzero (write the zero value as NULL — for optional foreign
// keys and other nullable columns held in non-pointer fields).
//
// For each model ormgen emits a schema value named after the table in
// CamelCase (Users) embedding *orm.Table[User] — so Users.Query(),
// Users.Find(id), Users.Create(&u) work — with one
// orm.Column[User, V] per tagged field.
package main

import (
	"bytes"
	"cmp"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"text/template"
)

const ormImport = "github.com/stubbedev/storm/orm"

type model struct {
	Struct, Table, Schema, Var string
	Connection, Key, KeyGen    string
	CreatedAt, UpdatedAt       string
	DeletedAt, Morph           string
	TimePrecision              int
	ModelField                 string
	Fields                     []field
}

type field struct {
	Name, Column, Type    string
	PK, Virtual, NullZero bool
}

// KeyType is the Go type of m's primary key field, or orm.NoKey.
func (m model) KeyType() string {
	for _, f := range m.Fields {
		if m.Key != "" && f.Column == m.Key {
			return f.Type
		}
	}
	return "orm.NoKey"
}

func main() {
	dir := flag.String("dir", ".", "package directory to scan")
	out := flag.String("out", "orm_gen.go", "output file name, relative to -dir")
	flag.Parse()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(*dir)
	if err != nil {
		log.Fatal(err)
	}
	var files []*ast.File
	var pkgName string
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") || e.Name() == *out || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(*dir, e.Name()), nil, parser.ParseComments)
		if err != nil {
			log.Fatal(err)
		}
		if pkgName == "" {
			pkgName = f.Name.Name
		} else if pkgName != f.Name.Name {
			log.Fatalf("ormgen: mixed packages in %s: %s and %s", *dir, pkgName, f.Name.Name)
		}
		files = append(files, f)
	}

	var models []model
	imports := map[string]string{} // path -> alias ("" if default)
	for _, file := range files {
		models = append(models, scanFile(file, imports)...)
	}
	if len(models) == 0 {
		log.Fatalf("ormgen: no //orm:table structs found in %s", *dir)
	}
	slices.SortFunc(models, func(a, b model) int { return strings.Compare(a.Struct, b.Struct) })

	src, err := render(pkgName, models, imports)
	if err != nil {
		log.Fatal(err)
	}
	if err := writeAtomic(filepath.Join(*dir, *out), src); err != nil {
		log.Fatal(err)
	}
}

// writeAtomic replaces path with src through a temporary file in the same
// directory, so a failed run never leaves a half-written source behind.
func writeAtomic(path string, src []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ormgen-*.go")
	if err != nil {
		return fmt.Errorf("ormgen: writing %s: %w", path, err)
	}
	_, err = tmp.Write(src)
	if err == nil {
		err = tmp.Chmod(0o644)
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("ormgen: writing %s: %w", path, err), os.Remove(tmp.Name()))
	}
	return nil
}

func scanFile(file *ast.File, imports map[string]string) []model {
	// Index the file's imports by the name they are referenced with.
	byName := map[string]*ast.ImportSpec{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			log.Fatalf("ormgen: bad import path %q: %v", spec.Path.Value, err)
		}
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		byName[name] = spec
	}

	var models []model
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			doc := ts.Doc
			if doc == nil {
				doc = gen.Doc
			}
			opts, ok := directive(doc)
			if !ok {
				continue
			}
			models = append(models, buildModel(ts.Name.Name, st, opts, byName, imports))
		}
	}
	return models
}

func buildModel(name string, st *ast.StructType, opts map[string]string, byName map[string]*ast.ImportSpec, imports map[string]string) model {
	fail := func(format string, args ...any) {
		log.Fatalf("ormgen: %s: %s", name, fmt.Sprintf(format, args...))
	}
	m := model{Struct: name, Table: opts["table"], Connection: opts["connection"], Morph: opts["morph"]}
	m.Schema = cmp.Or(opts["schema"], camel(m.Table))
	if m.Schema == name {
		fail("schema name %s collides with the struct; set schema=<Name>", m.Schema)
	}
	m.Var = strings.ToLower(m.Schema[:1]) + m.Schema[1:] + "Table"

	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			if sel, ok := f.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "Model" {
				if id, ok := sel.X.(*ast.Ident); ok && importPath(byName[id.Name]) == ormImport {
					m.ModelField = "Model"
				}
			}
			continue
		}
		if f.Tag == nil {
			continue
		}
		tag, _ := strconv.Unquote(f.Tag.Value)
		col, rest, _ := strings.Cut(reflect.StructTag(tag).Get("db"), ",")
		if col == "" || col == "-" {
			continue
		}
		tagOpts := strings.Split(rest, ",")
		collectImports(f.Type, byName, imports)
		for _, n := range f.Names {
			m.Fields = append(m.Fields, field{
				Name: n.Name, Column: col, Type: types.ExprString(f.Type),
				PK: slices.Contains(tagOpts, "pk"), Virtual: slices.Contains(tagOpts, "virtual"),
				NullZero: slices.Contains(tagOpts, "nullzero"),
			})
		}
	}
	if m.ModelField == "" {
		fail("must embed orm.Model")
	}

	col := func(name string) *field {
		for i := range m.Fields {
			if m.Fields[i].Column == name && !m.Fields[i].Virtual {
				return &m.Fields[i]
			}
		}
		return nil
	}

	// Primary key.
	m.Key = opts["key"]
	if m.Key == "" {
		for _, f := range m.Fields {
			if f.PK {
				m.Key = f.Column
			}
		}
	}
	if m.Key == "" && col("id") != nil {
		m.Key = "id"
	}
	if m.Key != "" && col(m.Key) == nil {
		fail("primary key column %q not found", m.Key)
	}
	switch opts["key_type"] {
	case "":
		m.KeyGen = "orm.KeyManual"
		if f := col(m.Key); f != nil && strings.Contains(f.Type, "int") {
			m.KeyGen = "orm.KeyAutoIncrement"
		}
	case "increments":
		m.KeyGen = "orm.KeyAutoIncrement"
	case "uuid":
		m.KeyGen = "orm.KeyUUID"
	case "ulid":
		m.KeyGen = "orm.KeyULID"
	case "manual":
		m.KeyGen = "orm.KeyManual"
	default:
		fail("unknown key_type %q", opts["key_type"])
	}

	// Timestamps and soft deletes.
	if opts["timestamps"] != "false" {
		if col("created_at") != nil {
			m.CreatedAt = "created_at"
		}
		if col("updated_at") != nil {
			m.UpdatedAt = "updated_at"
		}
	}
	if _, ok := opts["soft_deletes"]; ok {
		if col("deleted_at") == nil {
			fail("soft_deletes requires a deleted_at column")
		}
		m.DeletedAt = "deleted_at"
	}
	if p, ok := opts["time_precision"]; ok {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 9 {
			fail("time_precision must be 0 to 9, got %q", p)
		}
		m.TimePrecision = n
	}
	return m
}

// directive parses "//orm:table <name> [key=value | flag]...".
func directive(doc *ast.CommentGroup) (map[string]string, bool) {
	if doc == nil {
		return nil, false
	}
	known := []string{"key", "key_type", "connection", "timestamps", "soft_deletes", "morph", "schema", "time_precision"}
	for _, c := range doc.List {
		rest, ok := strings.CutPrefix(c.Text, "//orm:table ")
		if !ok {
			continue
		}
		args := strings.Fields(rest)
		if len(args) == 0 {
			log.Fatalf("ormgen: %q: missing table name", c.Text)
		}
		opts := map[string]string{"table": args[0]}
		for _, opt := range args[1:] {
			key, val, _ := strings.Cut(opt, "=")
			if !slices.Contains(known, key) {
				log.Fatalf("ormgen: %q: unknown option %q (known: %s)", c.Text, key, strings.Join(known, ", "))
			}
			opts[key] = val
		}
		return opts, true
	}
	return nil, false
}

func importPath(spec *ast.ImportSpec) string {
	if spec == nil {
		return ""
	}
	p, _ := strconv.Unquote(spec.Path.Value)
	return p
}

// collectImports records imports used by a field type, e.g. time for time.Time.
func collectImports(expr ast.Expr, byName map[string]*ast.ImportSpec, out map[string]string) {
	ast.Inspect(expr, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok {
			if spec, ok := byName[id.Name]; ok {
				alias := ""
				if spec.Name != nil {
					alias = spec.Name.Name
				}
				out[importPath(spec)] = alias
			}
		}
		return false
	})
}

func camel(s string) string {
	var b strings.Builder
	for part := range strings.SplitSeq(s, "_") {
		if part != "" {
			b.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
	}
	return b.String()
}

var tmpl = template.Must(template.New("").Parse(`// Code generated by ormgen. DO NOT EDIT.

package {{.Pkg}}

import (
{{- range $path, $alias := .Imports}}
	{{$alias}} "{{$path}}"
{{- end}}
)
{{range .Models}}{{$m := .}}
var {{.Var}} = &orm.Table[{{.Struct}}, {{.KeyType}}]{
	Name:       "{{.Table}}",
{{- if .Connection}}
	Connection: "{{.Connection}}",
{{- end}}
{{- if .Key}}
	PrimaryKey: "{{.Key}}",
	KeyType:    {{.KeyGen}},
{{- end}}
{{- if .CreatedAt}}
	CreatedAt:  "{{.CreatedAt}}",
{{- end}}
{{- if .UpdatedAt}}
	UpdatedAt:  "{{.UpdatedAt}}",
{{- end}}
{{- if .DeletedAt}}
	DeletedAt:  "{{.DeletedAt}}",
{{- end}}
{{- if .Morph}}
	MorphAlias: "{{.Morph}}",
{{- end}}
{{- if .TimePrecision}}
	TimePrecision: {{.TimePrecision}},
{{- end}}
{{- $nz := false}}{{range .Fields}}{{if .NullZero}}{{$nz = true}}{{end}}{{end}}
{{- if $nz}}
	NullZero: []string{ {{- $f1 := true}}{{range .Fields}}{{if .NullZero}}{{if not $f1}}, {{end}}"{{.Column}}"{{$f1 = false}}{{end}}{{end -}} },
{{- end}}
	Columns: []string{ {{- $first := true}}{{range .Fields}}{{if not .Virtual}}{{if not $first}}, {{end}}"{{.Column}}"{{$first = false}}{{end}}{{end -}} },
	Ptr: func(m *{{.Struct}}, column string) any {
		switch column {
{{- range .Fields}}
		case "{{.Column}}":
			return &m.{{.Name}}
{{- end}}
		}
		return nil
	},
	State: func(m *{{.Struct}}) *orm.Model { return &m.{{.ModelField}} },
}

// {{.Schema}}Schema describes the {{.Table}} table for {{.Struct}}.
type {{.Schema}}Schema struct {
	*orm.Table[{{.Struct}}, {{.KeyType}}]
{{- range .Fields}}
	// {{.Name}} is {{if .Virtual}}virtual, not stored: selected extras only{{else}}the {{$m.Table}}.{{.Column}} column{{end}} of {{$m.Struct}} ({{.Type}}).
	{{.Name}} orm.Column[{{$m.Struct}}, {{$m.KeyType}}, {{.Type}}]
{{- end}}
}

// {{.Schema}} is the entry point for the {{.Table}} table ({{.Struct}}):
// {{.Schema}}.Query(), {{.Schema}}.Find(id), {{.Schema}}.Where({{.Schema}}.{{(index .Fields 0).Name}}...) ...
var {{.Schema}} = {{.Schema}}Schema{
	Table: {{.Var}},
{{- range .Fields}}
	{{.Name}}: orm.{{if .Virtual}}NewVirtualColumn{{else}}NewColumn{{end}}({{$m.Var}}, "{{.Column}}", func(m *{{$m.Struct}}) *{{.Type}} { return &m.{{.Name}} }),
{{- end}}
}

// {{.Struct}}Query is a query over {{.Table}}, for scopes:
// func(q {{.Struct}}Query) {{.Struct}}Query.
type {{.Struct}}Query = orm.Query[{{.Struct}}, {{.KeyType}}]
{{end}}`))

func render(pkg string, models []model, imports map[string]string) ([]byte, error) {
	imports[ormImport] = ""
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, map[string]any{"Pkg": pkg, "Models": models, "Imports": imports}); err != nil {
		return nil, err
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("ormgen: formatting generated code: %w\n%s", err, buf.Bytes())
	}
	return src, nil
}
