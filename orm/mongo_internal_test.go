package orm

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestMongoStandaloneInsertUndo forces the standalone path (no
// transactions) on whatever deployment STORM_TEST_MONGO names: a batch
// failing part-way must leave none of its documents behind.
func TestMongoStandaloneInsertUndo(t *testing.T) {
	uri := os.Getenv("STORM_TEST_MONGO")
	if uri == "" {
		t.Skip("set STORM_TEST_MONGO to run")
	}
	ctx := context.Background()
	s, err := OpenMongo("mongo_standalone", uri, "stormundo"+strconv.FormatInt(time.Now().UnixNano()%100000, 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.db.Drop(ctx)
		_ = s.Close(ctx)
	})
	s.topologySet, s.canTransact = true, false
	if err := s.InsertDocs(ctx, "docs", "id", []map[string]any{{"id": "taken"}}); err != nil {
		t.Fatal(err)
	}
	err = s.InsertDocs(ctx, "docs", "id", []map[string]any{{"id": "a"}, {"id": "b"}, {"id": "taken"}, {"id": "c"}})
	if !IsUniqueViolation(err) {
		t.Fatalf("want a unique violation, got %v", err)
	}
	n, err := s.CountDocs(ctx, Plan{Table: "docs", Key: "id"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the failed batch left documents behind: %d stored, want 1", n)
	}
}

// TestMongoStandaloneUpsertUndo forces the standalone path: an upsert
// batch failing part-way must put back the documents it updated and remove
// those it inserted.
func TestMongoStandaloneUpsertUndo(t *testing.T) {
	uri := os.Getenv("STORM_TEST_MONGO")
	if uri == "" {
		t.Skip("set STORM_TEST_MONGO to run")
	}
	ctx := context.Background()
	s, err := OpenMongo("mongo_standalone_upsert", uri, "stormupsert"+strconv.FormatInt(time.Now().UnixNano()%100000, 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.db.Drop(ctx)
		_ = s.Close(ctx)
	})
	s.topologySet, s.canTransact = true, false
	if err := s.InsertDocs(ctx, "people", "id", []map[string]any{{"id": "x", "email": "x@example.com", "name": "old"}}); err != nil {
		t.Fatal(err)
	}
	err = s.UpsertDocs(ctx, "people", "id", []map[string]any{
		{"id": "a", "email": "a@example.com", "name": "A"},
		{"id": "y", "email": "x@example.com", "name": "new"},
		{"id": "x", "email": "other@example.com", "name": "clash"},
		{"id": "z", "email": "z@example.com", "name": "Z"},
	}, []string{"email"}, []string{"name"})
	if !IsUniqueViolation(err) {
		t.Fatalf("want a unique violation, got %v", err)
	}
	docs, err := s.FindDocs(ctx, Plan{Table: "people", Key: "id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0]["name"] != "old" || docs[0]["email"] != "x@example.com" {
		t.Fatalf("the failed upsert was not undone: %v", docs)
	}
}
