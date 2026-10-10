package orm

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// MongoStore runs the typed builder against MongoDB. Collections are named
// after the table; documents store each column under its db name, and the
// primary key is mirrored into _id for uniqueness and key lookups.
//
//	orm.OpenMongo("mongo", "mongodb://localhost:27017", "app")
//	//orm:table events connection=mongo key_type=uuid
//	err := Events.Query().Where(Events.Kind.Eq("click")).Limit(10).Get(ctx)
type MongoStore struct {
	client *mongo.Client
	db     *mongo.Database
}

// OpenMongo connects and registers a MongoDB connection.
func OpenMongo(name, uri, database string) (*MongoStore, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("orm: connecting to mongodb: %w", err)
	}
	s := &MongoStore{client: client, db: client.Database(database)}
	AddDocStore(name, s)
	return s, nil
}

// Close disconnects the client.
func (s *MongoStore) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

func (s *MongoStore) coll(table string) *mongo.Collection { return s.db.Collection(table) }

func (s *MongoStore) Engine() string { return "mongodb" }

// Transaction runs fn in a multi-document transaction, which MongoDB
// offers on replica sets and sharded clusters; operations on fn's ctx join
// it. fn runs once: a failure aborts, it is not retried.
func (s *MongoStore) Transaction(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	sess, err := s.client.StartSession()
	if err != nil {
		return fmt.Errorf("orm: mongo starting a session: %w", err)
	}
	defer sess.EndSession(ctx)
	if err := sess.StartTransaction(); err != nil {
		return fmt.Errorf("orm: mongo starting a transaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = sess.AbortTransaction(ctx)
			panic(p)
		}
	}()
	if err := fn(mongo.NewSessionContext(ctx, sess)); err != nil {
		if abortErr := sess.AbortTransaction(ctx); abortErr != nil {
			return errors.Join(err, fmt.Errorf("orm: mongo aborting a transaction: %w", abortErr))
		}
		return err
	}
	if err := sess.CommitTransaction(ctx); err != nil {
		return fmt.Errorf("orm: mongo committing a transaction: %w", err)
	}
	return nil
}

func (s *MongoStore) Collections(ctx context.Context) ([]string, error) {
	names, err := s.db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("orm: mongo listing collections: %w", err)
	}
	names = slices.DeleteFunc(names, func(n string) bool { return strings.HasPrefix(n, "system.") })
	slices.Sort(names)
	return names, nil
}

// CreateCollection creates the collection with a $jsonSchema validator
// enforcing the declared fields' types and presence, then its indexes.
// MongoDB has no vector index outside Atlas, so a vector is refused.
func (s *MongoStore) CreateCollection(ctx context.Context, c Collection) error {
	if c.Vector != nil {
		return &ErrNotTranslatable{Reason: "vector indexes on MongoDB (Atlas Vector Search is not wired up; use Qdrant or pgvector)"}
	}
	opts := options.CreateCollection()
	if len(c.Fields) > 0 {
		opts.SetValidator(mongoValidator(c.Fields))
	}
	if err := s.db.CreateCollection(ctx, c.Name, opts); err != nil {
		return fmt.Errorf("orm: mongo creating collection %s: %w", c.Name, err)
	}
	for _, idx := range c.Indexes {
		if err := s.CreateIndex(ctx, c.Name, idx); err != nil {
			return err
		}
	}
	return nil
}

func (s *MongoStore) DescribeCollection(ctx context.Context, name string) (Collection, error) {
	specs, err := s.db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil {
		return Collection{}, fmt.Errorf("orm: mongo describing collection %s: %w", name, err)
	}
	if len(specs) == 0 {
		return Collection{}, fmt.Errorf("orm: mongo collection %s does not exist", name)
	}
	c := Collection{Name: name}
	if c.Fields, err = mongoFields(specs[0].Options); err != nil {
		return Collection{}, fmt.Errorf("orm: mongo reading the validator of %s: %w", name, err)
	}
	cur, err := s.coll(name).Indexes().List(ctx)
	if err != nil {
		return Collection{}, fmt.Errorf("orm: mongo listing indexes of %s: %w", name, err)
	}
	var specsOut []struct {
		Name    string   `bson:"name"`
		Key     bson.D   `bson:"key"`
		Unique  bool     `bson:"unique"`
		Weights bson.Raw `bson:"weights"`
	}
	if err := cur.All(ctx, &specsOut); err != nil {
		return Collection{}, fmt.Errorf("orm: mongo reading indexes of %s: %w", name, err)
	}
	kinds := map[string]DocField{}
	for _, f := range c.Fields {
		kinds[f.Name] = f
	}
	field := func(n string) DocField {
		if f, ok := kinds[n]; ok {
			return f
		}
		return DocField{Name: n}
	}
	for _, spec := range specsOut {
		if spec.Name == "_id_" {
			continue
		}
		idx := DocIndex{Name: spec.Name, Unique: spec.Unique}
		if spec.Weights != nil {
			idx.Text = true
			elems, err := spec.Weights.Elements()
			if err != nil {
				return Collection{}, fmt.Errorf("orm: mongo reading text index %s: %w", spec.Name, err)
			}
			for _, e := range elems {
				idx.Fields = append(idx.Fields, field(e.Key()))
			}
		} else {
			for _, k := range spec.Key {
				idx.Fields = append(idx.Fields, field(k.Key))
			}
		}
		c.Indexes = append(c.Indexes, idx)
	}
	return c, nil
}

// DropCollection removes a collection; a missing one is not an error.
func (s *MongoStore) DropCollection(ctx context.Context, table string) error {
	if err := s.coll(table).Drop(ctx); err != nil {
		return fmt.Errorf("orm: mongo dropping collection %s: %w", table, err)
	}
	return nil
}

func (s *MongoStore) RenameCollection(ctx context.Context, from, to string) error {
	db := s.db.Name()
	err := s.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: db + "." + from},
		{Key: "to", Value: db + "." + to},
	}).Err()
	if err != nil {
		return fmt.Errorf("orm: mongo renaming collection %s to %s: %w", from, to, err)
	}
	return nil
}

// SetFields replaces the collection's validator with one for fields.
func (s *MongoStore) SetFields(ctx context.Context, collection string, fields []DocField) error {
	validator := any(bson.D{})
	if len(fields) > 0 {
		validator = mongoValidator(fields)
	}
	err := s.db.RunCommand(ctx, bson.D{{Key: "collMod", Value: collection}, {Key: "validator", Value: validator}}).Err()
	if err != nil {
		return fmt.Errorf("orm: mongo updating the validator of %s: %w", collection, err)
	}
	return nil
}

// bypassValidation lets a field rewrite through while the validator still
// describes the fields before the change.
var bypassValidation = options.UpdateMany().SetBypassDocumentValidation(true)

func (s *MongoStore) DropFields(ctx context.Context, collection string, fields []string) error {
	unset := make(bson.D, len(fields))
	for i, f := range fields {
		unset[i] = bson.E{Key: f, Value: ""}
	}
	if _, err := s.coll(collection).UpdateMany(ctx, bson.D{}, bson.D{{Key: "$unset", Value: unset}}, bypassValidation); err != nil {
		return fmt.Errorf("orm: mongo dropping fields of %s: %w", collection, err)
	}
	return nil
}

func (s *MongoStore) RenameField(ctx context.Context, collection, from, to string) error {
	_, err := s.coll(collection).UpdateMany(ctx, bson.D{}, bson.D{{Key: "$rename", Value: bson.D{{Key: from, Value: to}}}}, bypassValidation)
	if err != nil {
		return fmt.Errorf("orm: mongo renaming field %s of %s: %w", from, collection, err)
	}
	return nil
}

func (s *MongoStore) CreateIndex(ctx context.Context, collection string, idx DocIndex) error {
	keys := make(bson.D, len(idx.Fields))
	for i, f := range idx.Fields {
		keys[i] = bson.E{Key: f.Name, Value: map[bool]any{false: 1, true: "text"}[idx.Text]}
	}
	_, err := s.coll(collection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetName(idx.Name).SetUnique(idx.Unique),
	})
	if err != nil {
		return fmt.Errorf("orm: mongo creating index %s on %s: %w", idx.Name, collection, err)
	}
	return nil
}

func (s *MongoStore) DropIndex(ctx context.Context, collection string, idx DocIndex) error {
	if err := s.coll(collection).Indexes().DropOne(ctx, idx.Name); err != nil {
		return fmt.Errorf("orm: mongo dropping index %s on %s: %w", idx.Name, collection, err)
	}
	return nil
}

// mongoValidator is a $jsonSchema validator for fields: each declared
// field must hold its kind's BSON types (or null when nullable), and the
// non-nullable ones must be present. The kind is kept as the description,
// so DescribeCollection reads it back.
func mongoValidator(fields []DocField) bson.D {
	props := make(bson.D, 0, len(fields))
	required := bson.A{}
	for _, f := range fields {
		prop := bson.D{{Key: "description", Value: f.Kind.String()}}
		if types := mongoTypes(f.Kind); types != nil {
			if f.Nullable {
				types = append(types, "null")
			}
			prop = append(prop, bson.E{Key: "bsonType", Value: types})
		}
		if f.Unsigned {
			prop = append(prop, bson.E{Key: "minimum", Value: 0})
		}
		if len(f.Allowed) > 0 {
			allowed := make(bson.A, 0, len(f.Allowed)+1)
			for _, v := range f.Allowed {
				allowed = append(allowed, v)
			}
			if f.Nullable {
				allowed = append(allowed, nil)
			}
			prop = append(prop, bson.E{Key: "enum", Value: allowed})
		}
		props = append(props, bson.E{Key: f.Name, Value: prop})
		if !f.Nullable {
			required = append(required, f.Name)
		}
	}
	js := bson.D{{Key: "bsonType", Value: "object"}, {Key: "properties", Value: props}}
	if len(required) > 0 {
		js = append(js, bson.E{Key: "required", Value: required})
	}
	return bson.D{{Key: "$jsonSchema", Value: js}}
}

// mongoTypes are the BSON types the ORM stores a kind as: JSON casts and
// vectors arrive as their text form, maps and slices as documents and
// arrays.
func mongoTypes(k FieldKind) bson.A {
	switch k {
	case KindString:
		return bson.A{"string"}
	case KindInt:
		return bson.A{"int", "long"}
	case KindFloat:
		return bson.A{"double", "int", "long", "decimal"}
	case KindBool:
		return bson.A{"bool"}
	case KindTime:
		return bson.A{"date"}
	case KindJSON:
		return bson.A{"string", "object", "array"}
	case KindVector:
		return bson.A{"string", "array"}
	}
	return nil
}

// mongoFields reads the declared fields back from collection options; a
// collection without a validator declares none.
func mongoFields(opts bson.Raw) ([]DocField, error) {
	if opts == nil {
		return nil, nil
	}
	js, err := opts.LookupErr("validator", "$jsonSchema")
	if errors.Is(err, bsoncore.ErrElementNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var schema struct {
		Properties bson.Raw `bson:"properties"`
		Required   []string `bson:"required"`
	}
	if err := js.Unmarshal(&schema); err != nil {
		return nil, err
	}
	elems, err := schema.Properties.Elements()
	if err != nil {
		return nil, err
	}
	fields := make([]DocField, len(elems))
	for i, e := range elems {
		var prop struct {
			Description string `bson:"description"`
			Minimum     *int   `bson:"minimum"`
			Enum        []any  `bson:"enum"`
		}
		if err := e.Value().Unmarshal(&prop); err != nil {
			return nil, err
		}
		kind, ok := ParseFieldKind(prop.Description)
		if !ok {
			return nil, fmt.Errorf("field %s has no kind in its description", e.Key())
		}
		f := DocField{Name: e.Key(), Kind: kind, Nullable: !slices.Contains(schema.Required, e.Key()), Unsigned: prop.Minimum != nil}
		for _, v := range prop.Enum {
			if s, ok := v.(string); ok {
				f.Allowed = append(f.Allowed, s)
			}
		}
		fields[i] = f
	}
	return fields, nil
}

func (s *MongoStore) FindDocs(ctx context.Context, table string, p Plan) ([]map[string]any, error) {
	if p.Vector != nil {
		return nil, &ErrNotTranslatable{Reason: "vector search (MongoDB Atlas Vector Search is not wired up; use Qdrant or pgvector)"}
	}
	filter, err := mongoFilter(p.Where)
	if err != nil {
		return nil, err
	}
	opts := options.Find()
	if p.Limit > 0 {
		opts.SetLimit(int64(p.Limit))
	}
	if p.Offset > 0 {
		opts.SetSkip(int64(p.Offset))
	}
	if len(p.Orders) > 0 {
		sort := bson.D{}
		for _, o := range p.Orders {
			dir := 1
			if o.Desc {
				dir = -1
			}
			sort = append(sort, bson.E{Key: o.Column, Value: dir})
		}
		opts.SetSort(sort)
	}
	if len(p.Columns) > 0 {
		proj := bson.D{}
		for _, c := range p.Columns {
			proj = append(proj, bson.E{Key: c, Value: 1})
		}
		opts.SetProjection(proj)
	}
	cursor, err := s.coll(table).Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("orm: mongo find on %s: %w", table, err)
	}
	var docs []map[string]any
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("orm: mongo find on %s: %w", table, err)
	}
	return docs, nil
}

func (s *MongoStore) InsertDocs(ctx context.Context, table string, docs []map[string]any) error {
	if len(docs) == 0 {
		return nil
	}
	elems := make([]any, len(docs))
	for i, doc := range docs {
		d, err := mongoDoc(doc)
		if err != nil {
			return fmt.Errorf("orm: mongo insert into %s: %w", table, err)
		}
		elems[i] = d
	}
	if _, err := s.coll(table).InsertMany(ctx, elems); err != nil {
		return fmt.Errorf("orm: mongo insert into %s: %w", table, err)
	}
	return nil
}

// UpsertDocs replaces each document wholesale: models carry complete
// documents, so matching the keys and replacing equals updating every
// column, which is what Upsert's default update list asks for.
func (s *MongoStore) UpsertDocs(ctx context.Context, table string, docs []map[string]any, keys []string) error {
	for _, doc := range docs {
		filter, err := mongoDoc(pick(doc, keys))
		if err != nil {
			return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
		}
		replacement, err := mongoDoc(doc)
		if err != nil {
			return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
		}
		_, err = s.coll(table).UpdateOne(ctx, filter,
			mongo.Pipeline{bson.D{{Key: "$replaceWith", Value: replacement}}},
			options.UpdateOne().SetUpsert(true))
		if err != nil {
			return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
		}
	}
	return nil
}

func (s *MongoStore) UpdateDocs(ctx context.Context, table string, p Plan, u DocUpdate) (int64, error) {
	filter, err := mongoFilter(p.Where)
	if err != nil {
		return 0, err
	}
	update := bson.D{}
	for _, op := range []struct {
		name   string
		fields map[string]any
	}{{"$set", u.Set}, {"$inc", u.Inc}} {
		if len(op.fields) == 0 {
			continue
		}
		fields, err := mongoDoc(op.fields)
		if err != nil {
			return 0, fmt.Errorf("orm: mongo update on %s: %w", table, err)
		}
		update = append(update, bson.E{Key: op.name, Value: fields})
	}
	res, err := s.coll(table).UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, fmt.Errorf("orm: mongo update on %s: %w", table, err)
	}
	return res.MatchedCount, nil
}

func (s *MongoStore) DeleteDocs(ctx context.Context, table string, p Plan) (int64, error) {
	filter, err := mongoFilter(p.Where)
	if err != nil {
		return 0, err
	}
	res, err := s.coll(table).DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("orm: mongo delete on %s: %w", table, err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) CountDocs(ctx context.Context, table string, p Plan) (int64, error) {
	filter, err := mongoFilter(p.Where)
	if err != nil {
		return 0, err
	}
	if p.Vector != nil {
		return 0, &ErrNotTranslatable{Reason: "vector search on MongoDB"}
	}
	n, err := s.coll(table).CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("orm: mongo count on %s: %w", table, err)
	}
	return n, nil
}

// mongoDoc converts a builder document to BSON: values implementing
// driver.Valuer (orm.JSON, orm.Vector, orm.Encrypted) go through their
// value form, times stay native.
func mongoDoc(doc map[string]any) (bson.D, error) {
	d := make(bson.D, 0, len(doc))
	for _, k := range sortedKeys(doc) {
		v, err := bsonValue(doc[k])
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", k, err)
		}
		d = append(d, bson.E{Key: k, Value: v})
	}
	return d, nil
}

// pick is the fields of doc named by keys.
func pick(doc map[string]any, keys []string) map[string]any {
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		out[k] = doc[k]
	}
	return out
}

// bsonValue is v as MongoDB stores it. A driver.Valuer's error is
// returned, never papered over: Encrypted fails that way without a key,
// and storing the value as-is would write it in plain text.
func bsonValue(v any) (any, error) {
	switch v.(type) {
	case time.Time, string, bool, int, int32, int64, uint, uint32, uint64, float64, nil, []any, map[string]any:
		return v, nil
	}
	val, ok := v.(driver.Valuer)
	if !ok {
		return v, nil
	}
	raw, err := val.Value()
	if err != nil {
		return nil, fmt.Errorf("converting %T: %w", v, err)
	}
	return raw, nil
}

// mongoFilter compiles a plan condition to a BSON filter.
func mongoFilter(n Node) (bson.D, error) {
	if n == nil {
		return bson.D{}, nil
	}
	d, err := mongoCond(n)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func mongoCond(n Node) (bson.D, error) {
	switch c := n.(type) {
	case Field:
		v, err := bsonValue(c.Value)
		if err != nil {
			return nil, err
		}
		switch c.Op {
		case "=":
			return bson.D{{Key: c.Column, Value: v}}, nil
		case "<>", "!=":
			return bson.D{{Key: c.Column, Value: bson.D{{Key: "$ne", Value: v}}}}, nil
		case ">", ">=", "<", "<=":
			return bson.D{{Key: c.Column, Value: bson.D{{Key: "$" + mongoOp(c.Op), Value: v}}}}, nil
		case "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE":
			re, err := likeRegex(c.Value)
			if err != nil {
				return nil, err
			}
			match := bson.D{{Key: "$regex", Value: re}}
			if strings.Contains(c.Op, "ILIKE") {
				match = append(match, bson.E{Key: "$options", Value: "i"})
			}
			val := any(match)
			if strings.HasPrefix(c.Op, "NOT") {
				val = bson.D{{Key: "$not", Value: match}}
			}
			return bson.D{{Key: c.Column, Value: val}}, nil
		}
		return nil, &ErrNotTranslatable{Reason: "operator " + c.Op}
	case List:
		vals := make([]any, len(c.Values))
		for i, v := range c.Values {
			var err error
			if vals[i], err = bsonValue(v); err != nil {
				return nil, err
			}
		}
		op := "$in"
		if c.Not {
			op = "$nin"
		}
		return bson.D{{Key: c.Column, Value: bson.D{{Key: op, Value: vals}}}}, nil
	case Range:
		lo, err := bsonValue(c.Lo)
		if err != nil {
			return nil, err
		}
		hi, err := bsonValue(c.Hi)
		if err != nil {
			return nil, err
		}
		inner := bson.D{{Key: "$gte", Value: lo}, {Key: "$lte", Value: hi}}
		if c.Not {
			return bson.D{{Key: "$or", Value: bson.A{
				bson.D{{Key: c.Column, Value: bson.D{{Key: "$lt", Value: lo}}}},
				bson.D{{Key: c.Column, Value: bson.D{{Key: "$gt", Value: hi}}}},
			}}}, nil
		}
		return bson.D{{Key: c.Column, Value: inner}}, nil
	case NullTest:
		if c.Not {
			return bson.D{{Key: c.Column, Value: bson.D{{Key: "$ne", Value: nil}}}}, nil
		}
		return bson.D{{Key: c.Column, Value: nil}}, nil
	case Composite:
		parts := make(bson.A, 0, len(c.Parts))
		for _, p := range c.Parts {
			sub, err := mongoCond(p)
			if err != nil {
				return nil, err
			}
			if len(sub) == 0 {
				continue
			}
			parts = append(parts, sub)
		}
		if c.Or {
			return bson.D{{Key: "$or", Value: parts}}, nil
		}
		// AND: flatten sub-docs' keys; overlapping keys stay in $and.
		merged := bson.D{}
		conflict := false
		for _, sub := range parts {
			sd, _ := sub.(bson.D)
			for _, e := range sd {
				for _, m := range merged {
					if m.Key == e.Key {
						conflict = true
					}
				}
			}
			merged = append(merged, sd...)
		}
		if conflict {
			return bson.D{{Key: "$and", Value: parts}}, nil
		}
		return merged, nil
	case Negate:
		inner, err := mongoCond(c.Part)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: "$nor", Value: bson.A{inner}}}, nil
	case notTranslatable:
		return nil, &ErrNotTranslatable{Reason: "conditions on " + c.reason}
	}
	return nil, &ErrNotTranslatable{Reason: "this condition"}
}

func mongoOp(op string) string {
	return map[string]string{">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}[op]
}

// likeRegex turns a SQL LIKE pattern (with % and _) into an anchored
// case-sensitive regular expression.
func likeRegex(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", &ErrNotTranslatable{Reason: "LIKE on a non-string"}
	}
	var b strings.Builder
	b.WriteString("^")
	for _, r := range s {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			if strings.ContainsRune(`\.+*?()[]{}|^$`, r) {
				b.WriteRune('\\')
			}
			b.WriteRune(r)
		}
	}
	b.WriteString("$")
	return b.String(), nil
}
