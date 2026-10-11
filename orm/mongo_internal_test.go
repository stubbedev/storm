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
