package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "orm_gen.go")
	for _, src := range []string{"package a\n", "package b\n"} {
		if err := writeAtomic(path, []byte(src)); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != src {
			t.Fatalf("wrote %q, want %q", got, src)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("left %d files behind, want only orm_gen.go", len(entries))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("mode %v, want 0644", info.Mode().Perm())
	}
	if err := writeAtomic(filepath.Join(dir, "missing", "orm_gen.go"), []byte("package c\n")); err == nil {
		t.Fatal("writing into a missing directory succeeded")
	}
}
