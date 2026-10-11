package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// loaded is the package ormgen generates for: its files, as the build
// selects them for the current platform and tags, type-checked without the
// output file, which may be stale or missing.
type loaded struct {
	fset  *token.FileSet
	pkg   *types.Package
	files []*ast.File
}

type listed struct {
	ImportPath string
	Name       string
	Dir        string
	GoFiles    []string
	Export     string
	DepOnly    bool
	Error      *struct{ Err string }
}

// load lists dir with the go command, which applies build constraints and
// compiles the export data of every dependency, then type-checks the
// package's files against that export data. Type errors are tolerated: the
// other files may use what the output declares, and every field ormgen
// reads is checked on its own.
func load(dir, out string) (loaded, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-e", "-export", "-deps", "-json=ImportPath,Name,Dir,GoFiles,Export,DepOnly,Error", ".")
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, err := cmd.Output()
	if err != nil {
		return loaded{}, fmt.Errorf("ormgen: listing %s: %w\n%s", dir, err, stderr.Bytes())
	}
	exports := map[string]string{}
	var target *listed
	for dec := json.NewDecoder(bytes.NewReader(raw)); ; {
		var p listed
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return loaded{}, fmt.Errorf("ormgen: reading the package list of %s: %w", dir, err)
		}
		if !p.DepOnly {
			target = &p
			continue
		}
		if p.Error != nil {
			return loaded{}, fmt.Errorf("ormgen: dependency %s: %s", p.ImportPath, p.Error.Err)
		}
		exports[p.ImportPath] = p.Export
	}
	if target == nil {
		return loaded{}, fmt.Errorf("ormgen: %s holds no Go package", dir)
	}
	if target.Name == "" && target.Error != nil {
		return loaded{}, fmt.Errorf("ormgen: %s: %s", dir, target.Error.Err)
	}

	l := loaded{fset: token.NewFileSet()}
	for _, name := range target.GoFiles {
		if name == out {
			continue
		}
		f, err := parser.ParseFile(l.fset, filepath.Join(target.Dir, name), nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return loaded{}, fmt.Errorf("ormgen: %w", err)
		}
		l.files = append(l.files, f)
	}
	conf := types.Config{
		Importer: importer.ForCompiler(l.fset, "gc", func(path string) (io.ReadCloser, error) {
			file, ok := exports[path]
			if !ok || file == "" {
				return nil, fmt.Errorf("no export data for %s", path)
			}
			return os.Open(file) //nolint:gosec // the path comes from go list
		}),
		Error: func(error) {},
	}
	l.pkg, _ = conf.Check(target.ImportPath, l.fset, l.files, nil)
	return l, nil
}
