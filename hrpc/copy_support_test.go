// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func TestPutCellsWireFormats(t *testing.T) {
	cells := []PutCell{
		{[]byte("f"), []byte("q"), []byte("old"), 0},
		{[]byte("f"), []byte("q"), []byte("new"), 42},
		{[]byte("g"), []byte{}, []byte{}, 7},
	}
	table, key := []byte("ns:t"), []byte("row")
	put, err := NewPutCells(context.Background(), table, key, cells, Durability(SyncWal))
	if err != nil {
		t.Fatal(err)
	}
	table[0] = 'X'
	key[0] = 'X'
	cells[0].Value[0] = 'X'
	cells[1].Timestamp = 99
	copied := put.PutCells()
	copied[0].Value[0] = 'X'
	if put.Values() != nil {
		t.Fatal("cell versions must not be collapsed into Values")
	}
	put.SetRegion(mockRegionInfo("region"))
	request := put.ToProto().(*pb.MutateRequest)
	if string(request.Mutation.Row) != "row" || string(put.Table()) != "ns:t" {
		t.Fatal("aliased inputs")
	}
	if request.Mutation.Timestamp != nil {
		t.Fatal("unexpected whole-row timestamp")
	}
	if request.Mutation.GetDurability() != pb.MutationProto_SYNC_WAL {
		t.Fatal("lost durability")
	}
	expected := []PutCell{
		{[]byte("f"), []byte("q"), []byte("new"), 42},
		{[]byte("f"), []byte("q"), []byte("old"), 0},
		{[]byte("g"), []byte{}, []byte{}, 7},
	}
	n := 0
	for _, family := range request.Mutation.ColumnValue {
		for _, value := range family.QualifierValue {
			want := expected[n]
			n++
			if !bytes.Equal(family.Family, want.Family) ||
				!bytes.Equal(value.Qualifier, want.Qualifier) ||
				!bytes.Equal(value.Value, want.Value) ||
				value.Timestamp == nil ||
				*value.Timestamp != want.Timestamp {
				t.Fatalf("bad cell: %v", value)
			}
		}
	}
	if n != len(expected) {
		t.Fatal("lost versions")
	}
	wire, blocks, size := put.SerializeCellBlocks(nil)
	if wire.(*pb.MutateRequest).Mutation.GetAssociatedCellCount() != 3 ||
		len(blocks) != 1 ||
		int(size) != len(blocks[0]) {
		t.Fatal("bad cellblock envelope")
	}
	decoded, consumed, err := deserializeCellBlocksV2(blocks[0], 3)
	if err != nil || consumed != size {
		t.Fatalf("decode: %d %v", consumed, err)
	}
	for i, cell := range decoded {
		want := expected[i]
		if !bytes.Equal(cell.Row(), []byte("row")) ||
			!bytes.Equal(cell.Family(), want.Family) ||
			!bytes.Equal(cell.Qualifier(), want.Qualifier) ||
			!bytes.Equal(cell.Value(), want.Value) ||
			cell.Timestamp() != want.Timestamp ||
			cell.CellType() != pb.CellType_PUT {
			t.Fatalf("bad cellblock cell %v", cell)
		}
	}
	if !proto.Equal(request, put.ToProto()) {
		t.Fatal("serialization changed retry contents")
	}
}

func TestPutCellsValidation(t *testing.T) {
	valid := PutCell{Family: []byte("f"), Qualifier: []byte("q"), Timestamp: 1}
	for name, cells := range map[string][]PutCell{
		"empty":  nil,
		"family": {{Timestamp: 1}},
		"timestamp": {{Family: []byte("f"),
			Timestamp: math.MaxInt64}},
		"duplicate": {valid,
			valid},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPutCells(context.Background(),
				[]byte("t"),
				[]byte("r"),
				cells); err == nil {
				t.Fatal("accepted invalid cells")
			}
		})
	}
	for _, option := range []func(Call) error{Timestamp(time.Now()), TimestampUint64(0)} {
		if _, err := NewPutCells(context.Background(),
			[]byte("t"),
			[]byte("r"),
			[]PutCell{valid},
			option); err == nil {
			t.Fatal("accepted timestamp override")
		}
	}
}

func copyDescriptorFixture() *pb.TableSchema {
	return &pb.TableSchema{
		TableName: &pb.TableName{Qualifier: []byte("destination")},
		Attributes: []*pb.BytesBytesPair{{First: []byte("custom.table"),
			Second: []byte("value")}},
		Configuration: []*pb.NameStringPair{{Name: proto.String("table.config"),
			Value: proto.String("value")}},
		ColumnFamilies: []*pb.ColumnFamilySchema{{Name: []byte("f"),
			Attributes: []*pb.BytesBytesPair{{First: []byte("TTL"),
				Second: []byte("600")},
				{First: []byte("custom.family"),
					Second: []byte("value")}},
			Configuration: []*pb.NameStringPair{{Name: proto.String("family.config"),
				Value: proto.String("value")}},
		}},
	}
}
func TestCreateTableWithSchema(t *testing.T) {
	for _, ns := range []string{"", "default", "tenant"} {
		t.Run(ns, func(t *testing.T) {
			descriptor := copyDescriptorFixture()
			descriptor.TableName.Namespace = []byte(ns)
			expected := proto.Clone(descriptor).(*pb.TableSchema)
			if ns == "" {
				expected.TableName.Namespace = []byte("default")
			}
			keys := [][]byte{[]byte("m")}
			create, err := NewCreateTableWithSchema(context.Background(), descriptor, keys)
			if err != nil {
				t.Fatal(err)
			}
			descriptor.ColumnFamilies[0].Attributes[0].Second[0] = 'X'
			keys[0][0] = 'X'
			request := create.ToProto().(*pb.CreateTableRequest)
			if !proto.Equal(request.TableSchema, expected) || string(request.SplitKeys[0]) != "m" {
				t.Fatal("descriptor not preserved/copied")
			}
			nonce := request.GetNonce()
			request.TableSchema.ColumnFamilies[0].Name[0] = 'X'
			request.SplitKeys[0][0] = 'X'
			next := create.ToProto().(*pb.CreateTableRequest)
			if !proto.Equal(next.TableSchema, expected) ||
				string(next.SplitKeys[0]) != "m" ||
				next.GetNonce() != nonce ||
				next.NonceGroup == nil {
				t.Fatal("retry identity or contents changed")
			}
		})
	}
}
func TestCreateTableWithSchemaValidation(t *testing.T) {
	for name, modify := range map[string]func(*pb.TableSchema){
		"empty table":        func(d *pb.TableSchema) { d.TableName.Qualifier = nil },
		"embedded namespace": func(d *pb.TableSchema) { d.TableName.Qualifier = []byte("ns:t") },
		"duplicate family": func(d *pb.TableSchema) {
			d.ColumnFamilies = append(d.ColumnFamilies,
				d.ColumnFamilies[0])
		},
		"nil family":              func(d *pb.TableSchema) { d.ColumnFamilies[0] = nil },
		"missing required config": func(d *pb.TableSchema) { d.Configuration[0].Name = nil },
	} {
		t.Run(name, func(t *testing.T) {
			d := copyDescriptorFixture()
			modify(d)
			if _, err := NewCreateTableWithSchema(context.Background(), d, nil); err == nil {
				t.Fatal("accepted invalid descriptor")
			}
		})
	}
	for _, keys := range [][][]byte{{nil}, {[]byte("z"), []byte("a")}, {[]byte("a"), []byte("a")}} {
		if _, err := NewCreateTableWithSchema(context.Background(),
			copyDescriptorFixture(),
			keys); err == nil {
			t.Fatal("accepted invalid split keys")
		}
	}
}
func TestCreateTableRetainsCustomFamilyAttributes(t *testing.T) {
	request := NewCreateTable(context.Background(),
		[]byte("t"),
		map[string]map[string]string{"f": {"custom.family": "value",
			"TTL": "600"}}).ToProto().(*pb.CreateTableRequest)
	attributes := map[string]string{}
	for _, pair := range request.TableSchema.ColumnFamilies[0].Attributes {
		attributes[string(pair.First)] = string(pair.Second)
	}
	if attributes["custom.family"] != "value" ||
		attributes["TTL"] != "600" ||
		attributes["COMPRESSION"] != "NONE" {
		t.Fatalf("attributes: %v", attributes)
	}
}
