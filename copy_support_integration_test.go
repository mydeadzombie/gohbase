// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

//go:build integration

package gohbase_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

// Requires a disposable cluster and an existing HBASE_TEST_NAMESPACE, if set.
func TestCopySupportDescriptorsAndCellVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ns := os.Getenv("HBASE_TEST_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	qualifier := fmt.Sprintf("gohbase_copy_%d", time.Now().UnixNano())
	table := qualifier
	if ns != "default" {
		table = ns + ":" + qualifier
	}
	name := &pb.TableName{Namespace: []byte(ns), Qualifier: []byte(qualifier)}
	descriptor := &pb.TableSchema{TableName: name, ColumnFamilies: []*pb.ColumnFamilySchema{{
		Name: []byte("f"), Attributes: []*pb.BytesBytesPair{
			{First: []byte("VERSIONS"), Second: []byte("3")},
			{First: []byte("custom.copy.attribute"), Second: []byte("preserved")},
		},
	}}}
	create, err := hrpc.NewCreateTableWithSchema(ctx, descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	admin := gohbase.NewAdminClient(*host)
	defer admin.Close()
	// Register cleanup before RPC: a timed-out create may have committed.
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		_ = admin.DisableTable(hrpc.NewDisableTable(cleanup, []byte(table)))
		_ = admin.DeleteTable(hrpc.NewDeleteTable(cleanup, []byte(table)))
	}()
	if err = admin.CreateTable(create); err != nil {
		t.Fatal(err)
	}
	describe,
		err := hrpc.NewGetTableDescriptors(ctx,
		hrpc.DescribeTableNames([]*pb.TableName{name}))
	if err != nil {
		t.Fatal(err)
	}
	schemas, err := admin.GetTableSchema(describe)
	if err != nil || len(schemas) != 1 {
		t.Fatalf("describe: %v %v", schemas, err)
	}
	found := false
	for _, family := range schemas[0].ColumnFamilies {
		if string(family.Name) == "f" {
			for _, pair := range family.Attributes {
				if string(pair.First) == "custom.copy.attribute" &&
					string(pair.Second) == "preserved" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("custom family attribute not preserved")
	}
	data := gohbase.NewClient(*host)
	defer data.Close()
	now := uint64(time.Now().UnixMilli())
	put, err := hrpc.NewPutCells(ctx, []byte(table), []byte("row"), []hrpc.PutCell{
		{Family: []byte("f"), Qualifier: []byte("q"), Value: []byte("old"), Timestamp: now - 2000},
		{Family: []byte("f"), Qualifier: []byte("q"), Value: []byte("new"), Timestamp: now - 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = data.Put(put); err != nil {
		t.Fatal(err)
	}
	get, err := hrpc.NewGetStr(ctx, table, "row", hrpc.MaxVersions(3))
	if err != nil {
		t.Fatal(err)
	}
	result, err := data.Get(get)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || len(result.Cells) != 2 {
		t.Fatalf("expected two versions, got %v", result)
	}
	want := map[uint64]string{now - 2000: "old", now - 1000: "new"}
	for _, cell := range result.Cells {
		if cell.Timestamp == nil || want[*cell.Timestamp] != string(cell.Value) {
			t.Fatalf("unexpected cell %v", cell)
		}
		delete(want, *cell.Timestamp)
	}
	if len(want) != 0 {
		t.Fatal("missing versions")
	}
}
