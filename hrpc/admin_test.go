// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"testing"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func TestAdminNamespaces(t *testing.T) {
	for _, name := range []string{"table", "default:table", "tenant:table"} {
		ns := "default"
		if name == "tenant:table" {
			ns = "tenant"
		}
		ctx := context.Background()
		calls := []Call{
			NewCreateTable(ctx, []byte(name), map[string]map[string]string{"cf": nil}),
			NewEnableTable(ctx, []byte(name)), NewDisableTable(ctx, []byte(name)),
			NewDeleteTable(ctx, []byte(name)),
		}
		for _, call := range calls {
			var table *pb.TableName
			switch req := call.ToProto().(type) {
			case *pb.CreateTableRequest:
				table = req.TableSchema.TableName
			case *pb.EnableTableRequest:
				table = req.TableName
			case *pb.DisableTableRequest:
				table = req.TableName
			case *pb.DeleteTableRequest:
				table = req.TableName
			}
			if string(table.Namespace) != ns || string(table.Qualifier) != "table" {
				t.Errorf("%s(%s): %v", call.Name(), name, table)
			}
		}
	}
}

func TestAdminMutations(t *testing.T) {
	ctx := context.Background()
	table := []byte("tenant:table")
	family := &pb.ColumnFamilySchema{Name: []byte("cf"), Attributes: []*pb.BytesBytesPair{
		{First: []byte("VERSIONS"), Second: []byte("1")},
	}, Configuration: []*pb.NameStringPair{
		{Name: proto.String("custom"), Value: proto.String("value")},
	}}
	add, err := NewAddColumnFamily(ctx, table, family)
	if err != nil {
		t.Fatal(err)
	}
	modify, err := NewModifyColumnFamily(ctx, table, family)
	if err != nil {
		t.Fatal(err)
	}
	truncate, err := NewTruncateTable(ctx, table, true)
	if err != nil {
		t.Fatal(err)
	}
	// Mutation requests own their descriptor and table bytes.
	family.Name[0] = 'x'
	table[0] = 'x'
	a := add.ToProto().(*pb.AddColumnRequest)
	m := modify.ToProto().(*pb.ModifyColumnRequest)
	tr := truncate.ToProto().(*pb.TruncateTableRequest)
	if string(a.TableName.Namespace) != "tenant" || string(a.ColumnFamilies.Name) != "cf" {
		t.Fatal("request aliases caller memory", a)
	}
	if !proto.Equal(a.ColumnFamilies, m.ColumnFamilies) ||
		len(m.ColumnFamilies.Configuration) != 1 {
		t.Fatal("descriptor not preserved")
	}
	if !tr.GetPreserveSplits() || string(tr.TableName.Namespace) != "tenant" {
		t.Fatal(tr)
	}
	if a.GetNonce() == m.GetNonce() || m.GetNonce() == tr.GetNonce() {
		t.Fatal("distinct calls share nonce")
	}
	for _, call := range []Call{add, modify, truncate} {
		if !proto.Equal(call.ToProto(), call.ToProto()) {
			t.Fatal("retry changed request")
		}
		if _, err := proto.Marshal(call.ToProto()); err != nil {
			t.Fatal(err)
		}
	}
	noSplits, _ := NewTruncateTable(ctx, []byte("table"), false)
	if noSplits.ToProto().(*pb.TruncateTableRequest).GetPreserveSplits() {
		t.Fatal("unexpected preserveSplits")
	}
}

func TestAdminMutationValidation(t *testing.T) {
	for _, table := range []string{"", ":table", "ns:", "a:b:c"} {
		if _, err := NewTruncateTable(context.Background(), []byte(table), true); err == nil {
			t.Errorf("accepted invalid table %q", table)
		}
	}
	for _, family := range []*pb.ColumnFamilySchema{nil, {}} {
		if _, err := NewAddColumnFamily(context.Background(), []byte("table"), family); err == nil {
			t.Fatal("accepted unnamed column family")
		}
	}
}

func TestAdminProcedureIDsDecode(t *testing.T) {
	// Field 1 (varint), value 42: HBase 2.x procedure response wire format.
	for _, response := range []proto.Message{
		&pb.AddColumnResponse{}, &pb.ModifyColumnResponse{}, &pb.TruncateTableResponse{},
		&pb.ModifyTableResponse{},
	} {
		if err := proto.Unmarshal([]byte{8, 42}, response); err != nil {
			t.Fatal(err)
		}
		field := response.ProtoReflect().Descriptor().Fields().ByNumber(1)
		if field == nil || response.ProtoReflect().Get(field).Uint() != 42 {
			t.Fatal(response)
		}
	}
}

func TestReplicationTableNamespaces(t *testing.T) {
	for _, name := range []string{"table", "default:table", "tenant:table"} {
		t.Run(name, func(t *testing.T) {
			request, err := NewAddReplicationPeer(context.Background(), "peer", "zk:2181:/hbase",
				AddReplicationPeerReplicateTableFamilies([]byte(name), []byte("cf")))
			if err != nil {
				t.Fatal(err)
			}
			config := request.ToProto().(*pb.AddReplicationPeerRequest).PeerConfig
			if len(config.TableCfs) != 1 {
				t.Fatalf("unexpected table filters: %v", config.TableCfs)
			}
			filter := config.TableCfs[0]
			namespace := "default"
			if name == "tenant:table" {
				namespace = "tenant"
			}
			if string(filter.TableName.Namespace) != namespace ||
				string(filter.TableName.Qualifier) != "table" {
				t.Fatalf("incorrect table name: %v", filter.TableName)
			}
			if len(filter.Families) != 1 || string(filter.Families[0]) != "cf" {
				t.Fatalf("column-family filter changed: %v", filter.Families)
			}
		})
	}
}

func TestModifyTableDescriptor(t *testing.T) {
	for _, namespace := range []string{"", "default", "tenant"} {
		descriptor := &pb.TableSchema{
			TableName:      &pb.TableName{Namespace: []byte(namespace), Qualifier: []byte("table")},
			ColumnFamilies: []*pb.ColumnFamilySchema{{Name: []byte("cf")}},
			Attributes: []*pb.BytesBytesPair{
				{First: []byte("custom"), Second: []byte("value")},
			},
			Configuration: []*pb.NameStringPair{
				{Name: proto.String("setting"), Value: proto.String("value")},
			},
		}
		request, err := NewModifyTable(context.Background(), descriptor)
		if err != nil {
			t.Fatal(err)
		}
		expected := proto.Clone(descriptor).(*pb.TableSchema)
		if namespace == "" {
			expected.TableName.Namespace = []byte("default")
		}
		descriptor.ColumnFamilies[0].Name[0] = 'x'
		descriptor.Attributes[0].Second[0] = 'x'
		wire := request.ToProto().(*pb.ModifyTableRequest)
		if !proto.Equal(wire.TableSchema, expected) ||
			!proto.Equal(wire.TableName, expected.TableName) {
			t.Fatal("descriptor changed or aliased", wire)
		}
		if !proto.Equal(wire, request.ToProto()) || wire.GetNonce() == 0 {
			t.Fatal("unstable retry identity")
		}
	}
}

func TestModifyTableValidation(t *testing.T) {
	for _, descriptor := range []*pb.TableSchema{
		nil, {}, {TableName: &pb.TableName{Qualifier: []byte("table")}},
		{TableName: &pb.TableName{Qualifier: []byte("ns:table")},
			ColumnFamilies: []*pb.ColumnFamilySchema{{Name: []byte("cf")}}},
		{TableName: &pb.TableName{Qualifier: []byte("table")},
			ColumnFamilies: []*pb.ColumnFamilySchema{nil}},
		{TableName: &pb.TableName{Qualifier: []byte("table")},
			ColumnFamilies: []*pb.ColumnFamilySchema{{Name: []byte("cf")}, {Name: []byte("cf")}}},
	} {
		if _, err := NewModifyTable(context.Background(), descriptor); err == nil {
			t.Errorf("accepted invalid descriptor: %v", descriptor)
		}
	}
}
