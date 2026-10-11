package orm

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
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
//	events, err := Events.Query().Where(Events.Kind.Eq("click")).Limit(10).Get()
type MongoStore struct {
	client *mongo.Client
	db     *mongo.Database

	topologyMu  sync.Mutex
	topologySet bool // canTransact is known
	canTransact bool // a replica set or sharded cluster
}

// transactional reports whether the deployment runs multi-document
// transactions (a replica set or sharded cluster), asking it once.
func (s *MongoStore) transactional(ctx context.Context) (bool, error) {
	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()
	if s.topologySet {
		return s.canTransact, nil
	}
	var hello struct {
		SetName string `bson:"setName"`
		Msg     string `bson:"msg"`
	}
	if err := s.db.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return false, fmt.Errorf("orm: mongo reading the deployment topology: %w", err)
	}
	s.topologySet, s.canTransact = true, hello.SetName != "" || hello.Msg == "isdbgrid"
	return s.canTransact, nil
}

// allOrNothing runs a write of n documents so it applies whole: as is for
// one document or inside the caller's transaction, in its own transaction
// where the deployment has them, and otherwise as is with undo, which
// reverts what a failed write managed to apply.
func (s *MongoStore) allOrNothing(ctx context.Context, n int, write func(context.Context) error, undo func(context.Context, error) error) error {
	if n < 2 || mongo.SessionFromContext(ctx) != nil {
		return write(ctx)
	}
	tx, err := s.transactional(ctx)
	if err != nil {
		return err
	}
	if tx {
		return s.Transaction(ctx, write)
	}
	if err := write(ctx); err != nil {
		return errors.Join(err, undo(ctx, err))
	}
	return nil
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
// enforcing the declared fields' types and presence, then its indexes and
// its vector search index. Vector search needs MongoDB's search engine
// (Atlas, or a local Atlas deployment); on a deployment without it the
// collection is dropped again and the vector refused.
func (s *MongoStore) CreateCollection(ctx context.Context, c Collection) error {
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
	if c.Vector == nil {
		return nil
	}
	if err := s.createVectorIndex(ctx, c); err != nil {
		return errors.Join(err, s.DropCollection(context.WithoutCancel(ctx), c.Name))
	}
	return nil
}

// mongoSimilarities are the search index similarities of the metrics.
var mongoSimilarities = map[VectorMetric]string{L2: "euclidean", Cosine: "cosine", InnerProduct: "dotProduct"}

// mongoVectorIndex names a collection's vector search index, as the
// schema package names an HNSW index.
func mongoVectorIndex(collection, field string) string { return collection + "_" + field + "_hnsw" }

// vectorIndexWait bounds how long a migration waits for a new vector
// search index to become queryable.
const vectorIndexWait = 2 * time.Minute

// searchUnsupported turns MongoDB's "no search engine" error into a
// refusal naming what is missing.
func searchUnsupported(err error) error {
	var ce mongo.CommandError
	if errors.As(err, &ce) && ce.Code == 31082 {
		return &ErrNotTranslatable{Reason: "vector search on this MongoDB deployment, which has no search engine (use Atlas or a local Atlas deployment)"}
	}
	return err
}

// createVectorIndex creates c's vector search index, with every declared
// scalar field and the key as filter fields so that a vector search can
// be filtered, and waits until it is queryable.
func (s *MongoStore) createVectorIndex(ctx context.Context, c Collection) error {
	v := c.Vector
	fields := bson.A{bson.D{
		{Key: "type", Value: "vector"},
		{Key: "path", Value: v.Field},
		{Key: "numDimensions", Value: v.Size},
		{Key: "similarity", Value: mongoSimilarities[v.Metric]},
	}}
	fields = append(fields, bson.D{{Key: "type", Value: "filter"}, {Key: "path", Value: "_id"}})
	for _, f := range c.Fields {
		switch f.Kind {
		case KindString, KindInt, KindFloat, KindBool, KindTime:
			fields = append(fields, bson.D{{Key: "type", Value: "filter"}, {Key: "path", Value: f.Name}})
		}
	}
	name := mongoVectorIndex(c.Name, v.Field)
	_, err := s.coll(c.Name).SearchIndexes().CreateOne(ctx, mongo.SearchIndexModel{
		Definition: bson.D{{Key: "fields", Value: fields}},
		Options:    options.SearchIndexes().SetName(name).SetType("vectorSearch"),
	})
	if err != nil {
		return fmt.Errorf("orm: mongo creating vector index %s: %w", name, searchUnsupported(err))
	}
	ctx, cancel := context.WithTimeout(ctx, vectorIndexWait)
	defer cancel()
	for {
		ready, err := s.vectorIndexReady(ctx, c.Name, name)
		if err != nil || ready {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("orm: mongo vector index %s did not become queryable: %w", name, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (s *MongoStore) vectorIndexReady(ctx context.Context, collection, name string) (bool, error) {
	cur, err := s.coll(collection).SearchIndexes().List(ctx, options.SearchIndexes().SetName(name))
	if err != nil {
		return false, fmt.Errorf("orm: mongo reading vector index %s: %w", name, searchUnsupported(err))
	}
	var specs []struct {
		Queryable bool `bson:"queryable"`
	}
	if err := cur.All(ctx, &specs); err != nil {
		return false, fmt.Errorf("orm: mongo reading vector index %s: %w", name, err)
	}
	return len(specs) == 1 && specs[0].Queryable, nil
}

// vectorOf reads collection's vector from its vector search index; nil
// when it has none, or the deployment has no search engine to hold one.
func (s *MongoStore) vectorOf(ctx context.Context, collection string) (*DocVector, error) {
	cur, err := s.coll(collection).SearchIndexes().List(ctx, nil)
	if err != nil {
		if IsNotTranslatable(searchUnsupported(err)) {
			return nil, nil
		}
		return nil, fmt.Errorf("orm: mongo listing search indexes of %s: %w", collection, err)
	}
	var specs []struct {
		Type             string `bson:"type"`
		LatestDefinition struct {
			Fields []struct {
				Type          string `bson:"type"`
				Path          string `bson:"path"`
				NumDimensions int    `bson:"numDimensions"`
				Similarity    string `bson:"similarity"`
			} `bson:"fields"`
		} `bson:"latestDefinition"`
	}
	if err := cur.All(ctx, &specs); err != nil {
		return nil, fmt.Errorf("orm: mongo reading search indexes of %s: %w", collection, err)
	}
	for _, spec := range specs {
		if spec.Type != "vectorSearch" {
			continue
		}
		for _, f := range spec.LatestDefinition.Fields {
			if f.Type != "vector" {
				continue
			}
			for m, sim := range mongoSimilarities {
				if sim == f.Similarity {
					return &DocVector{Field: f.Path, Size: f.NumDimensions, Metric: m}, nil
				}
			}
			return nil, fmt.Errorf("orm: mongo vector index on %s.%s has an unknown similarity %q", collection, f.Path, f.Similarity)
		}
	}
	return nil, nil
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
	if c.Vector, err = s.vectorOf(ctx, name); err != nil {
		return Collection{}, err
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
		return bson.A{"array"}
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

// mongoNullsKey prefixes the fields a find adds to sort NULLs and missing
// values first or last, where MongoDB's own order puts them elsewhere.
const mongoNullsKey = "__storm_nulls_"

func (s *MongoStore) FindDocs(ctx context.Context, p Plan) ([]map[string]any, error) {
	filter, err := mongoFilter(p)
	if err != nil {
		return nil, err
	}
	if p.Vector != nil {
		return s.vectorSearch(ctx, p, filter)
	}
	sort, nullKeys := bson.D{}, bson.D{}
	for i, o := range p.Orders {
		field := mongoField(o.Column, p.Key)
		if o.Nulls != NullsDefault {
			key := fmt.Sprintf("%s%d", mongoNullsKey, i)
			nullKeys = append(nullKeys, bson.E{Key: key, Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$" + field, nil}}}, nil}}}, 1, 0,
			}}}})
			sort = append(sort, bson.E{Key: key, Value: map[Nulls]int{NullsLast: 1, NullsFirst: -1}[o.Nulls]})
		}
		sort = append(sort, bson.E{Key: field, Value: map[bool]int{false: 1, true: -1}[o.Desc]})
	}
	var cursor *mongo.Cursor
	if len(nullKeys) > 0 {
		pipeline := mongo.Pipeline{{{Key: "$match", Value: filter}}, {{Key: "$addFields", Value: nullKeys}}, {{Key: "$sort", Value: sort}}}
		if p.Offset > 0 {
			pipeline = append(pipeline, bson.D{{Key: "$skip", Value: int64(p.Offset)}})
		}
		if p.Limit > 0 {
			pipeline = append(pipeline, bson.D{{Key: "$limit", Value: int64(p.Limit)}})
		}
		project := bson.D{}
		for _, c := range p.Columns {
			project = append(project, bson.E{Key: c, Value: 1})
		}
		if len(project) == 0 {
			for _, k := range nullKeys {
				project = append(project, bson.E{Key: k.Key, Value: 0})
			}
		}
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: project}})
		cursor, err = s.coll(p.Table).Aggregate(ctx, pipeline)
	} else {
		opts := options.Find()
		if p.Limit > 0 {
			opts.SetLimit(int64(p.Limit))
		}
		if p.Offset > 0 {
			opts.SetSkip(int64(p.Offset))
		}
		if len(sort) > 0 {
			opts.SetSort(sort)
		}
		if len(p.Columns) > 0 {
			proj := bson.D{}
			for _, c := range p.Columns {
				proj = append(proj, bson.E{Key: c, Value: 1})
			}
			opts.SetProjection(proj)
		}
		cursor, err = s.coll(p.Table).Find(ctx, filter, opts)
	}
	if err != nil {
		return nil, fmt.Errorf("orm: mongo find on %s: %w", p.Table, err)
	}
	var docs []map[string]any
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("orm: mongo find on %s: %w", p.Table, err)
	}
	for _, doc := range docs {
		delete(doc, "_id")
		for k, v := range doc {
			doc[k] = fromBSON(v)
		}
	}
	return docs, nil
}

// vectorSearch ranks the matching documents by distance from p.Vector with
// $vectorSearch, an approximate nearest-neighbour search over the
// collection's vector index. It needs a Limit (the k of the search), the
// metric of the index, and no other order.
func (s *MongoStore) vectorSearch(ctx context.Context, p Plan, filter bson.D) ([]map[string]any, error) {
	v := p.Vector
	switch {
	case p.Limit == 0:
		return nil, &ErrNotTranslatable{Reason: "a vector search without a Limit (MongoDB searches for the k nearest)"}
	case len(p.Orders) > 0:
		return nil, &ErrNotTranslatable{Reason: "ordering after a vector search on MongoDB"}
	}
	have, err := s.vectorOf(ctx, p.Table)
	switch {
	case err != nil:
		return nil, err
	case have == nil:
		return nil, &ErrNotTranslatable{Reason: "a vector search on " + p.Table + ", which has no vector search index (or the deployment no search engine)"}
	case have.Field != v.Column:
		return nil, fmt.Errorf("orm: mongo vector search on %s.%s: the vector index is on %s", p.Table, v.Column, have.Field)
	case have.Metric != v.Metric:
		return nil, fmt.Errorf("orm: mongo vector search on %s.%s: the index measures %s, not the %s the order asks for", p.Table, v.Column, mongoSimilarities[have.Metric], mongoSimilarities[v.Metric])
	}
	k := p.Offset + p.Limit
	search := bson.D{
		{Key: "index", Value: mongoVectorIndex(p.Table, v.Column)},
		{Key: "path", Value: v.Column},
		{Key: "queryVector", Value: v.Floats},
		{Key: "numCandidates", Value: min(max(k*20, 100), 10000)},
		{Key: "limit", Value: k},
	}
	if len(filter) > 0 {
		search = append(search, bson.E{Key: "filter", Value: filter})
	}
	pipeline := mongo.Pipeline{{{Key: "$vectorSearch", Value: search}}}
	if p.Offset > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$skip", Value: int64(p.Offset)}})
	}
	if len(p.Columns) > 0 {
		project := bson.D{}
		for _, c := range p.Columns {
			project = append(project, bson.E{Key: c, Value: 1})
		}
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: project}})
	}
	cursor, err := s.coll(p.Table).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("orm: mongo vector search on %s: %w", p.Table, searchUnsupported(err))
	}
	var docs []map[string]any
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("orm: mongo vector search on %s: %w", p.Table, err)
	}
	for _, doc := range docs {
		delete(doc, "_id")
		for k, v := range doc {
			doc[k] = fromBSON(v)
		}
	}
	return docs, nil
}

// fromBSON turns the driver's BSON types into the plain values SQL rows
// carry: nested documents into maps, arrays into slices, binary into
// []byte, datetimes into UTC times and decimals into exact text.
func fromBSON(v any) any {
	switch x := v.(type) {
	case bson.D:
		m := make(map[string]any, len(x))
		for _, e := range x {
			m[e.Key] = fromBSON(e.Value)
		}
		return m
	case map[string]any:
		for k, e := range x {
			x[k] = fromBSON(e)
		}
		return x
	case bson.A:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = fromBSON(e)
		}
		return out
	case bson.Binary:
		return x.Data
	case bson.DateTime:
		return x.Time().UTC()
	case bson.Decimal128:
		return x.String()
	}
	return v
}

// InsertDocs stores each document under _id = its key, so MongoDB's own
// unique _id index rejects a taken key.
func (s *MongoStore) InsertDocs(ctx context.Context, table, key string, docs []map[string]any) error {
	if len(docs) == 0 {
		return nil
	}
	elems := make([]any, len(docs))
	ids := make(bson.A, len(docs))
	for i, doc := range docs {
		d, err := mongoKeyed(doc, key)
		if err != nil {
			return fmt.Errorf("orm: mongo insert into %s: %w", table, err)
		}
		elems[i], ids[i] = d, d[0].Value
	}
	coll := s.coll(table)
	err := s.allOrNothing(ctx, len(elems), func(ctx context.Context) error {
		_, err := coll.InsertMany(ctx, elems)
		return err
	}, func(ctx context.Context, err error) error { return undoInserted(ctx, coll, ids, err) })
	switch {
	case err == nil:
		return nil
	case mongo.IsDuplicateKeyError(err):
		return &DuplicateKeyError{Table: table, Key: key, Err: err}
	}
	return fmt.Errorf("orm: mongo insert into %s: %w", table, err)
}

// undoInserted deletes the documents (by their _id values, ids) an
// ordered InsertMany stored before the write error err reports; without a
// write error what was stored is unknown, which it reports instead.
func undoInserted(ctx context.Context, coll *mongo.Collection, ids bson.A, err error) error {
	bulk, ok := errors.AsType[mongo.BulkWriteException](err)
	if !ok || len(bulk.WriteErrors) == 0 {
		return errors.New("the insert failed without a write error, so some documents may be stored")
	}
	stored := ids[:slices.MinFunc(bulk.WriteErrors, func(a, b mongo.BulkWriteError) int { return a.Index - b.Index }).Index]
	if len(stored) == 0 {
		return nil
	}
	if _, err := coll.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: stored}}}}); err != nil {
		return fmt.Errorf("undoing the %d documents stored before the failure: %w", len(stored), err)
	}
	return nil
}

// mongoMatching reads the documents any of filters matches, by _id.
func mongoMatching(ctx context.Context, coll *mongo.Collection, filters bson.A) (map[any]bson.D, error) {
	cur, err := coll.Find(ctx, bson.D{{Key: "$or", Value: filters}})
	if err != nil {
		return nil, fmt.Errorf("reading the documents the upsert matches: %w", err)
	}
	var docs []bson.D
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("reading the documents the upsert matches: %w", err)
	}
	byID := make(map[any]bson.D, len(docs))
	for _, d := range docs {
		byID[mongoID(d)] = d
	}
	return byID, nil
}

func mongoID(d bson.D) any {
	for _, e := range d {
		if e.Key == "_id" {
			return e.Value
		}
	}
	return nil
}

// undoUpserted reverts what an ordered bulk upsert applied before the
// write error err: the documents its filters matched before (before) get
// their earlier versions back, and the documents it inserted are deleted.
// Without a write error every filter may have applied, so every one is
// reverted.
func undoUpserted(ctx context.Context, coll *mongo.Collection, filters bson.A, before map[any]bson.D, err error) error {
	applied := filters
	if bulk, ok := errors.AsType[mongo.BulkWriteException](err); ok && len(bulk.WriteErrors) > 0 {
		applied = filters[:slices.MinFunc(bulk.WriteErrors, func(a, b mongo.BulkWriteError) int { return a.Index - b.Index }).Index]
	}
	if len(applied) == 0 {
		return nil
	}
	ids := make(bson.A, 0, len(before))
	for id := range before {
		ids = append(ids, id)
	}
	touched, err := mongoMatching(ctx, coll, append(slices.Clone(applied), bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}))
	if err != nil {
		return fmt.Errorf("undoing the upsert: %w", err)
	}
	var restores []mongo.WriteModel
	var inserted bson.A
	for id := range touched {
		if old, ok := before[id]; ok {
			restores = append(restores, mongo.NewReplaceOneModel().SetFilter(bson.D{{Key: "_id", Value: id}}).SetReplacement(old))
		} else {
			inserted = append(inserted, id)
		}
	}
	if len(inserted) > 0 {
		if _, err := coll.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: inserted}}}}); err != nil {
			return fmt.Errorf("undoing the %d documents the upsert inserted: %w", len(inserted), err)
		}
	}
	if len(restores) > 0 {
		if _, err := coll.BulkWrite(ctx, restores); err != nil {
			return fmt.Errorf("restoring the %d documents the upsert updated: %w", len(restores), err)
		}
	}
	return nil
}

// UpsertDocs writes every document in one bulk write: a document matching
// on uniqueBy gets only its update columns set, a new one is inserted
// whole under _id = its key.
func (s *MongoStore) UpsertDocs(ctx context.Context, table, key string, docs []map[string]any, uniqueBy, update []string) error {
	if len(docs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, len(docs))
	filters := make(bson.A, len(docs))
	for i, doc := range docs {
		filter := bson.D{}
		for _, c := range uniqueBy {
			if err := mongoFieldName(c); err != nil {
				return err
			}
			v, err := bsonValue(doc[c])
			if err != nil {
				return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
			}
			filter = append(filter, bson.E{Key: mongoField(c, key), Value: v})
		}
		set := map[string]any{}
		onInsert := map[string]any{}
		for c, v := range doc {
			if c != key && slices.Contains(update, c) {
				set[c] = v
			} else {
				onInsert[c] = v
			}
		}
		insert, err := mongoKeyed(onInsert, key)
		if err != nil {
			return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
		}
		changes := bson.D{{Key: "$setOnInsert", Value: insert}}
		if len(set) > 0 {
			fields, err := mongoDoc(set)
			if err != nil {
				return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
			}
			changes = append(bson.D{{Key: "$set", Value: fields}}, changes...)
		}
		models[i] = mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(changes).SetUpsert(true)
		filters[i] = filter
	}
	coll := s.coll(table)
	var before map[any]bson.D
	err := s.allOrNothing(ctx, len(models), func(ctx context.Context) error {
		if len(models) > 1 && mongo.SessionFromContext(ctx) == nil {
			var err error
			if before, err = mongoMatching(ctx, coll, filters); err != nil {
				return err
			}
		}
		_, err := coll.BulkWrite(ctx, models)
		return err
	}, func(ctx context.Context, err error) error { return undoUpserted(ctx, coll, filters, before, err) })
	if err != nil {
		return fmt.Errorf("orm: mongo upsert into %s: %w", table, err)
	}
	return nil
}

// mongoKeyed is doc as BSON with its key mirrored into _id.
func mongoKeyed(doc map[string]any, key string) (bson.D, error) {
	id, ok := doc[key]
	if !ok || isZero(id) {
		return nil, fmt.Errorf("every document needs a key in %q", key)
	}
	d, err := mongoDoc(doc)
	if err != nil {
		return nil, err
	}
	v, err := bsonValue(id)
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", key, err)
	}
	return append(bson.D{{Key: "_id", Value: v}}, d...), nil
}

// mongoField is the BSON field a column filters and sorts on: the key
// column is mirrored into _id, which MongoDB always indexes.
func mongoField(column, key string) string {
	if column == key {
		return "_id"
	}
	return column
}

func (s *MongoStore) UpdateDocs(ctx context.Context, p Plan, u DocUpdate) (int64, error) {
	table := p.Table
	filter, err := mongoFilter(p)
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

func (s *MongoStore) DeleteDocs(ctx context.Context, p Plan) (int64, error) {
	table := p.Table
	filter, err := mongoFilter(p)
	if err != nil {
		return 0, err
	}
	res, err := s.coll(table).DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("orm: mongo delete on %s: %w", table, err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) CountDocs(ctx context.Context, p Plan) (int64, error) {
	table := p.Table
	filter, err := mongoFilter(p)
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

// AggregateDocs runs fn over column as a $match + $group pipeline.
func (s *MongoStore) AggregateDocs(ctx context.Context, p Plan, fn Aggregate, column string) (any, error) {
	table := p.Table
	if p.Vector != nil {
		return nil, &ErrNotTranslatable{Reason: "vector search on MongoDB"}
	}
	if err := mongoFieldName(column); err != nil {
		return nil, err
	}
	op, ok := map[Aggregate]string{AggregateSum: "$sum", AggregateAvg: "$avg", AggregateMin: "$min", AggregateMax: "$max"}[fn]
	if !ok {
		return nil, fmt.Errorf("orm: mongo aggregate on %s: unknown %s", table, fn)
	}
	filter, err := mongoFilter(p)
	if err != nil {
		return nil, err
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: nil}, {Key: "v", Value: bson.D{{Key: op, Value: "$" + column}}}}}},
	}
	cursor, err := s.coll(table).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("orm: mongo aggregate on %s: %w", table, err)
	}
	var out []struct {
		V any `bson:"v"`
	}
	if err := cursor.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("orm: mongo aggregate on %s: %w", table, err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return fromBSON(out[0].V), nil
}

// mongoDoc converts a builder document to BSON: values implementing
// driver.Valuer (orm.JSON, orm.Vector, orm.Encrypted) go through their
// value form, times stay native.
func mongoDoc(doc map[string]any) (bson.D, error) {
	d := make(bson.D, 0, len(doc))
	for _, k := range sortedKeys(doc) {
		if err := mongoFieldName(k); err != nil {
			return nil, err
		}
		v, err := bsonValue(doc[k])
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", k, err)
		}
		d = append(d, bson.E{Key: k, Value: v})
	}
	return d, nil
}

// bsonValue is v as MongoDB stores it. A driver.Valuer's error is
// returned, never papered over: Encrypted fails that way without a key,
// and storing the value as-is would write it in plain text.
func bsonValue(v any) (any, error) {
	switch x := v.(type) {
	case time.Time:
		return mongoTime(x)
	case *time.Time:
		if x == nil {
			return nil, nil
		}
		return mongoTime(*x)
	case string, bool, int, int32, int64, uint, uint32, uint64, float64, nil, []any, map[string]any:
		return v, nil
	}
	if vec, ok := v.(interface{ Floats() []float64 }); ok {
		fs := vec.Floats()
		if fs == nil {
			return nil, nil
		}
		arr := make(bson.A, len(fs))
		for i, f := range fs {
			arr[i] = f
		}
		return arr, nil
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

// mongoTime refuses a time finer than BSON's millisecond datetime rather
// than storing it truncated.
func mongoTime(t time.Time) (time.Time, error) {
	if t.Nanosecond()%int(time.Millisecond) != 0 {
		return t, fmt.Errorf("%s is finer than the millisecond MongoDB stores; truncate it (t.Truncate(time.Millisecond)) or use Timestamps(3)", t.Format(time.RFC3339Nano))
	}
	return t.UTC(), nil
}

// mongoFilter compiles a plan condition to a BSON filter.
func mongoFilter(p Plan) (bson.D, error) {
	for _, c := range planColumns(p) {
		if err := mongoFieldName(c); err != nil {
			return nil, err
		}
	}
	if p.Where == nil {
		return bson.D{}, nil
	}
	return mongoCond(lowerNegations(p.Where, false), p.Key)
}

// mongoFieldName refuses a column name MongoDB would read as an operator
// ($where, $ne, ...) anywhere in its path, so a name taken from input
// cannot change what a query does.
func mongoFieldName(name string) error {
	if name == "" || strings.ContainsRune(name, 0) {
		return fmt.Errorf("orm: %q is not a MongoDB field name", name)
	}
	for part := range strings.SplitSeq(name, ".") {
		if strings.HasPrefix(part, "$") {
			return fmt.Errorf("orm: %q is not a MongoDB field name: a $ part is an operator", name)
		}
	}
	return nil
}

// mongoNever matches no document.
var mongoNever = bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{}}}}}

// mongoCond compiles a negation-lowered condition. Negative leaves also
// exclude null and missing fields, which SQL's NULL never satisfies, and a
// comparison with nil matches nothing, as col = NULL does in SQL.
func mongoCond(n Node, key string) (bson.D, error) {
	switch c := n.(type) {
	case Field:
		v, err := bsonValue(c.Value)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return mongoNever, nil
		}
		switch c.Op {
		case "=":
			return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$eq", Value: v}}}}, nil
		case "<>", "!=":
			return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$nin", Value: bson.A{v, nil}}}}}, nil
		case ">", ">=", "<", "<=":
			return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$" + mongoOp(c.Op), Value: v}}}}, nil
		case "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE":
			re, err := likeRegex(c.Value)
			if err != nil {
				return nil, err
			}
			options := "s"
			if strings.Contains(c.Op, "ILIKE") {
				options = "si"
			}
			match := bson.D{{Key: "$regex", Value: re}, {Key: "$options", Value: options}}
			if strings.HasPrefix(c.Op, "NOT") {
				return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$not", Value: match}, {Key: "$ne", Value: nil}}}}, nil
			}
			return bson.D{{Key: mongoField(c.Column, key), Value: match}}, nil
		}
		return nil, &ErrNotTranslatable{Reason: "operator " + c.Op}
	case List:
		if len(c.Values) == 0 {
			if c.Not {
				return bson.D{}, nil
			}
			return mongoNever, nil
		}
		vals := make(bson.A, len(c.Values), len(c.Values)+1)
		for i, v := range c.Values {
			var err error
			if vals[i], err = bsonValue(v); err != nil {
				return nil, err
			}
		}
		if c.Not {
			return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$nin", Value: append(vals, nil)}}}}, nil
		}
		return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$in", Value: vals}}}}, nil
	case Range:
		lo, err := bsonValue(c.Lo)
		if err != nil {
			return nil, err
		}
		hi, err := bsonValue(c.Hi)
		if err != nil {
			return nil, err
		}
		if c.Not {
			return bson.D{{Key: "$or", Value: bson.A{
				bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$lt", Value: lo}}}},
				bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$gt", Value: hi}}}},
			}}}, nil
		}
		return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$gte", Value: lo}, {Key: "$lte", Value: hi}}}}, nil
	case NullTest:
		if c.Not {
			return bson.D{{Key: mongoField(c.Column, key), Value: bson.D{{Key: "$ne", Value: nil}}}}, nil
		}
		return bson.D{{Key: mongoField(c.Column, key), Value: nil}}, nil
	case Composite:
		parts := make(bson.A, 0, len(c.Parts))
		for _, p := range c.Parts {
			sub, err := mongoCond(p, key)
			if err != nil {
				return nil, err
			}
			if len(sub) == 0 {
				if c.Or {
					return bson.D{}, nil
				}
				continue
			}
			parts = append(parts, sub)
		}
		switch {
		case len(parts) == 0 && c.Or:
			return mongoNever, nil
		case len(parts) == 0:
			return bson.D{}, nil
		case c.Or:
			return bson.D{{Key: "$or", Value: parts}}, nil
		}
		return bson.D{{Key: "$and", Value: parts}}, nil
	case Negate:
		return nil, &ErrNotTranslatable{Reason: "negating this condition"}
	case notTranslatable:
		return nil, &ErrNotTranslatable{Reason: "conditions on " + c.reason}
	}
	return nil, &ErrNotTranslatable{Reason: "this condition"}
}

func mongoOp(op string) string {
	return map[string]string{">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}[op]
}

// likeRegex turns a SQL LIKE pattern (%, _ and \ escapes) into an
// anchored regular expression; the s option lets wildcards cross newlines
// as they do in SQL.
func likeRegex(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", &ErrNotTranslatable{Reason: "LIKE on a non-string"}
	}
	var b strings.Builder
	b.WriteString("^")
	escaped := false
	for _, r := range s {
		switch {
		case escaped:
			escaped = false
			b.WriteString(regexp.QuoteMeta(string(r)))
		case r == '\\':
			escaped = true
		case r == '%':
			b.WriteString(".*")
		case r == '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return b.String(), nil
}
