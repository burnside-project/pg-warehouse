package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseHeaderMetadata(t *testing.T) {
	sql := `-- name: my_model
-- materialized: view
-- contract: orders_v1
-- tags: finance, daily
SELECT 1`

	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "my_model" {
		t.Errorf("name = %q, want %q", result.Name, "my_model")
	}
	if result.Materialization != "view" {
		t.Errorf("materialization = %q, want %q", result.Materialization, "view")
	}
	if result.Contract != "orders_v1" {
		t.Errorf("contract = %q, want %q", result.Contract, "orders_v1")
	}
	if len(result.Tags) != 2 || result.Tags[0] != "finance" || result.Tags[1] != "daily" {
		t.Errorf("tags = %v, want [finance daily]", result.Tags)
	}
}

func TestParseDefaultMaterialization(t *testing.T) {
	sql := `-- name: simple
SELECT 1`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Materialization != "table" {
		t.Errorf("materialization = %q, want %q", result.Materialization, "table")
	}
}

func TestParseMaterializationAlias(t *testing.T) {
	sql := `-- materialization: incremental
SELECT 1`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Materialization != "incremental" {
		t.Errorf("materialization = %q, want %q", result.Materialization, "incremental")
	}
}

func TestParseSingleRef(t *testing.T) {
	sql := `SELECT * FROM {{ ref('orders') }}`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Refs) != 1 || result.Refs[0] != "orders" {
		t.Errorf("refs = %v, want [orders]", result.Refs)
	}
}

func TestParseMultipleRefs(t *testing.T) {
	sql := `SELECT *
FROM {{ ref('orders') }} o
JOIN {{ ref('customers') }} c ON o.cust_id = c.id
JOIN {{ref('products')}} p ON o.prod_id = p.id`

	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Refs) != 3 {
		t.Fatalf("got %d refs, want 3", len(result.Refs))
	}
	expected := []string{"orders", "customers", "products"}
	for i, want := range expected {
		if result.Refs[i] != want {
			t.Errorf("refs[%d] = %q, want %q", i, result.Refs[i], want)
		}
	}
}

func TestParseSingleSource(t *testing.T) {
	sql := `SELECT * FROM {{ source('raw', 'events') }}`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Sources) != 1 {
		t.Fatalf("got %d sources, want 1", len(result.Sources))
	}
	if result.Sources[0].Schema != "raw" || result.Sources[0].Table != "events" {
		t.Errorf("source = %+v, want {Schema:raw Table:events}", result.Sources[0])
	}
}

func TestParseMultipleSources(t *testing.T) {
	sql := `SELECT * FROM {{ source('raw', 'events') }}
UNION ALL
SELECT * FROM {{ source('staging', 'users') }}`

	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Sources) != 2 {
		t.Fatalf("got %d sources, want 2", len(result.Sources))
	}
}

func TestParseSourceSingleArg(t *testing.T) {
	sql := `SELECT * FROM {{ source('raw_schema') }}`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Sources) != 1 {
		t.Fatalf("got %d sources, want 1", len(result.Sources))
	}
	if result.Sources[0].Schema != "raw_schema" || result.Sources[0].Table != "" {
		t.Errorf("source = %+v, want {Schema:raw_schema Table:}", result.Sources[0])
	}
}

func TestParseNoRefsOrSources(t *testing.T) {
	sql := `SELECT 1 AS one`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Refs) != 0 {
		t.Errorf("refs = %v, want empty", result.Refs)
	}
	if len(result.Sources) != 0 {
		t.Errorf("sources = %v, want empty", result.Sources)
	}
}

func TestParseCommentsWithRefLikeText(t *testing.T) {
	// ref-like text inside a SQL block comment should still be extracted;
	// the parser does a naive regex scan, so it picks up refs in comments too.
	sql := `-- This references {{ ref('commented_model') }} in a comment
SELECT * FROM {{ ref('real_model') }}`

	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Both should be captured (parser is intentionally naive).
	if len(result.Refs) != 2 {
		t.Errorf("got %d refs, want 2", len(result.Refs))
	}
}

func TestChecksumConsistent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	if err := os.WriteFile(path, []byte("SELECT 1"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	sum1, err := Checksum(path)
	if err != nil {
		t.Fatalf("checksum 1: %v", err)
	}
	sum2, err := Checksum(path)
	if err != nil {
		t.Fatalf("checksum 2: %v", err)
	}
	if sum1 != sum2 {
		t.Errorf("checksums differ: %s vs %s", sum1, sum2)
	}
	if len(sum1) != 64 {
		t.Errorf("checksum length = %d, want 64 hex chars", len(sum1))
	}
}

func TestChecksumDifferentContent(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "a.sql")
	path2 := filepath.Join(dir, "b.sql")
	_ = os.WriteFile(path1, []byte("SELECT 1"), 0644)
	_ = os.WriteFile(path2, []byte("SELECT 2"), 0644)

	sum1, _ := Checksum(path1)
	sum2, _ := Checksum(path2)
	if sum1 == sum2 {
		t.Error("different content produced same checksum")
	}
}
