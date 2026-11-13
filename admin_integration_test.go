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

// Set HBASE_TEST_NAMESPACE to an existing, disposable test namespace to also
// exercise non-default namespace administration. Each case owns a unique table.
func TestAdminColumnFamiliesAndTruncate(t *testing.T) {
	for _, preserve := range []bool{false, true} {
		t.Run(fmt.Sprint(preserve), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			name := fmt.Sprintf("gohbase_admin_%d", time.Now().UnixNano())
			if ns := os.Getenv("HBASE_TEST_NAMESPACE"); ns != "" && ns != "default" {
				name = ns + ":" + name
			}
			ac := gohbase.NewAdminClient(*host)
			defer ac.Close()
			data := gohbase.NewClient(*host)
			defer data.Close()
			create := hrpc.NewCreateTable(ctx, []byte(name),
				map[string]map[string]string{"cf": nil},
				hrpc.SplitKeys([][]byte{[]byte("m")}))
			if err := ac.CreateTable(create); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				_ = ac.DisableTable(hrpc.NewDisableTable(cleanup, []byte(name)))
				if err := ac.DeleteTable(hrpc.NewDeleteTable(cleanup, []byte(name))); err != nil {
					t.Errorf("cleanup %s: %v", name, err)
				}
			}()
			family := &pb.ColumnFamilySchema{Name: []byte("TypeDatahbase")}
			add, err := hrpc.NewAddColumnFamily(ctx, []byte(name), family)
			if err != nil {
				t.Fatal(err)
			}
			if err := ac.AddColumnFamily(add); err != nil {
				t.Fatal(err)
			}
			// Read the complete descriptor before changing an attribute.
			describe, err := hrpc.NewGetTableDescriptors(ctx, hrpc.DescribeRegex("^"+name+"$"))
			if err != nil {
				t.Fatal(err)
			}
			schemas, err := ac.GetTableSchema(describe)
			if err != nil || len(schemas) != 1 {
				t.Fatalf("schemas=%v err=%v", schemas, err)
			}
			var descriptor *pb.ColumnFamilySchema
			for _, cf := range schemas[0].ColumnFamilies {
				if string(cf.Name) == "TypeDatahbase" {
					descriptor = cf
				}
			}
			if descriptor == nil {
				t.Fatal("added family missing")
			}
			found := false
			for _, attr := range descriptor.Attributes {
				if string(attr.First) == "VERSIONS" {
					attr.Second = []byte("2")
					found = true
				}
			}
			if !found {
				descriptor.Attributes = append(descriptor.Attributes,
					&pb.BytesBytesPair{First: []byte("VERSIONS"), Second: []byte("2")})
			}
			modify, err := hrpc.NewModifyColumnFamily(ctx, []byte(name), descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if err := ac.ModifyColumnFamily(modify); err != nil {
				t.Fatal(err)
			}
			schemas, err = ac.GetTableSchema(describe)
			if err != nil {
				t.Fatal(err)
			}
			changed := false
			for _, table := range schemas {
				for _, cf := range table.ColumnFamilies {
					if string(cf.Name) == "TypeDatahbase" {
						for _, attr := range cf.Attributes {
							changed = changed || string(attr.First) == "VERSIONS" &&
								string(attr.Second) == "2"
						}
					}
				}
			}
			if !changed {
				t.Fatal("column family modification not visible")
			}
			if len(schemas) != 1 {
				t.Fatalf("expected one table descriptor, got %d", len(schemas))
			}
			schemas[0].Attributes = append(schemas[0].Attributes, &pb.BytesBytesPair{
				First: []byte("gohbase.test.attribute"), Second: []byte("retained"),
			})
			modifyTable, err := hrpc.NewModifyTable(ctx, schemas[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := ac.ModifyTable(modifyTable); err != nil {
				t.Fatal(err)
			}
			schemas, err = ac.GetTableSchema(describe)
			if err != nil || len(schemas) != 1 {
				t.Fatalf("table descriptors: %v, %v", schemas, err)
			}
			attributeFound := false
			for _, attribute := range schemas[0].Attributes {
				attributeFound = attributeFound ||
					string(attribute.First) == "gohbase.test.attribute" &&
						string(attribute.Second) == "retained"
			}
			if !attributeFound || len(schemas[0].ColumnFamilies) != 2 {
				t.Fatal("table attribute update lost attributes or families")
			}
			put, err := hrpc.NewPutStr(ctx, name, "a",
				map[string]map[string][]byte{"TypeDatahbase": {"x": []byte("value")}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := data.Put(put); err != nil {
				t.Fatal(err)
			}
			if err := ac.DisableTable(hrpc.NewDisableTable(ctx, []byte(name))); err != nil {
				t.Fatal(err)
			}
			truncate, err := hrpc.NewTruncateTable(ctx, []byte(name), preserve)
			if err != nil {
				t.Fatal(err)
			}
			if err := ac.TruncateTable(truncate); err != nil {
				t.Fatal(err)
			}
			get, err := hrpc.NewGetStr(ctx, name, "a")
			if err != nil {
				t.Fatal(err)
			}
			result, err := data.Get(get)
			if err != nil || len(result.Cells) != 0 {
				t.Fatalf("row survived: %v %v", result, err)
			}
			regions, err := getTableRegions(name)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if preserve {
				want = 2
			}
			if len(regions) != want {
				t.Fatalf("got %d regions, want %d", len(regions), want)
			}
			if err := ac.DisableTable(hrpc.NewDisableTable(ctx, []byte(name))); err != nil {
				t.Fatal(err)
			}
			if err := ac.EnableTable(hrpc.NewEnableTable(ctx, []byte(name))); err != nil {
				t.Fatal(err)
			}
		})
	}
}
