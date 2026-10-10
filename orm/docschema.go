package orm

import "context"

// DocSchema is the schema side of a document store — the collections,
// fields and indexes migrations manage through the schema package, so a
// MongoDB or Qdrant connection migrates with the same Create / Table /
// Drop calls as a SQL one. MongoStore and QdrantStore implement it; an
// operation a store cannot express fails with ErrNotTranslatable.
type DocSchema interface {
	// Engine names the store: "mongodb" or "qdrant".
	Engine() string
	Collections(ctx context.Context) ([]string, error)
	// CreateCollection creates c with its fields, indexes and vector.
	CreateCollection(ctx context.Context, c Collection) error
	// DescribeCollection reads back what CreateCollection and the
	// alterations declared, as far as the store records it.
	DescribeCollection(ctx context.Context, name string) (Collection, error)
	// DropCollection removes a collection; a missing one is not an error.
	DropCollection(ctx context.Context, name string) error
	RenameCollection(ctx context.Context, from, to string) error
	// SetFields replaces the declared fields (MongoDB's validator); stores
	// without declared fields accept it as is.
	SetFields(ctx context.Context, collection string, fields []DocField) error
	// DropFields removes fields from every document.
	DropFields(ctx context.Context, collection string, fields []string) error
	// RenameField renames a field in every document.
	RenameField(ctx context.Context, collection, from, to string) error
	CreateIndex(ctx context.Context, collection string, idx DocIndex) error
	DropIndex(ctx context.Context, collection string, idx DocIndex) error
}

// Collection is a document collection's declared shape.
type Collection struct {
	Name    string
	Fields  []DocField
	Indexes []DocIndex
	Vector  *DocVector // the collection's vector, where the store has one
}

// DocField is a declared document field: Unsigned numbers must not be
// negative, and a field with Allowed values holds one of them (an enum).
type DocField struct {
	Name     string
	Kind     FieldKind
	Nullable bool
	Unsigned bool
	Allowed  []string
}

// FieldKind is the type family of a document field, as the ORM stores
// values in documents.
type FieldKind int

const (
	KindAny FieldKind = iota
	KindString
	KindInt
	KindFloat
	KindBool
	KindTime
	KindJSON
	KindVector
)

var fieldKindNames = [...]string{
	KindAny: "any", KindString: "string", KindInt: "int", KindFloat: "float",
	KindBool: "bool", KindTime: "time", KindJSON: "json", KindVector: "vector",
}

func (k FieldKind) String() string { return fieldKindNames[k] }

// ParseFieldKind is the inverse of FieldKind.String.
func ParseFieldKind(s string) (FieldKind, bool) {
	for k, name := range fieldKindNames {
		if name == s {
			return FieldKind(k), true
		}
	}
	return KindAny, false
}

// DocIndex is an index over document fields; Text makes it a full text
// index, Primary the collection's key.
type DocIndex struct {
	Name    string
	Fields  []DocField
	Unique  bool
	Text    bool
	Primary bool
}

// FieldNames lists the index's field names in order.
func (i DocIndex) FieldNames() []string {
	names := make([]string, len(i.Fields))
	for j, f := range i.Fields {
		names[j] = f.Name
	}
	return names
}

var (
	_ DocSchema = (*MongoStore)(nil)
	_ DocSchema = (*QdrantStore)(nil)
)

// DocVector is a collection's vector: its size and distance metric.
type DocVector struct {
	Field  string
	Size   int
	Metric VectorMetric
}
