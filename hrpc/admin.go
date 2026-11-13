// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// tableNameProto preserves explicit namespaces and defaults only bare table names.
func tableNameProto(table []byte) *pb.TableName {
	namespace, qualifier, found := bytes.Cut(table, []byte(":"))
	if !found {
		namespace, qualifier = []byte("default"), table
	}
	return &pb.TableName{Namespace: namespace, Qualifier: qualifier}
}

var adminNonceGroup = func() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return binary.BigEndian.Uint64(b[:]) & ((1 << 63) - 1)
}()

var adminNonce atomic.Uint64

// adminMutation keeps nonce identity stable across retries of the same call.
type adminMutation struct {
	base
	nonce uint64
}

func newAdminMutation(ctx context.Context, table []byte) (adminMutation, error) {
	tn := tableNameProto(table)
	if len(tn.Namespace) == 0 || len(tn.Qualifier) == 0 || bytes.Count(table, []byte(":")) > 1 {
		return adminMutation{}, errors.New("expected table or namespace:table")
	}
	return adminMutation{
		base:  base{ctx: ctx, table: bytes.Clone(table), resultch: make(chan RPCResult, 1)},
		nonce: adminNonce.Add(1),
	}, nil
}

// AddColumnFamily adds one column-family descriptor to an existing table.
type AddColumnFamily struct {
	adminMutation
	family *pb.ColumnFamilySchema
}

// NewAddColumnFamily snapshots the descriptor. HBase supplies omitted defaults.
func NewAddColumnFamily(ctx context.Context, table []byte,
	family *pb.ColumnFamilySchema) (*AddColumnFamily, error) {
	m, err := newAdminMutation(ctx, table)
	if err != nil {
		return nil, err
	}
	if family == nil || len(family.Name) == 0 {
		return nil, errors.New("column family name is required")
	}
	return &AddColumnFamily{m, proto.Clone(family).(*pb.ColumnFamilySchema)}, nil
}

func (c *AddColumnFamily) Name() string        { return "AddColumn" }
func (c *AddColumnFamily) Description() string { return c.Name() }
func (c *AddColumnFamily) ToProto() proto.Message {
	return &pb.AddColumnRequest{
		TableName: tableNameProto(c.table), ColumnFamilies: c.family,
		NonceGroup: proto.Uint64(adminNonceGroup), Nonce: proto.Uint64(c.nonce),
	}
}
func (c *AddColumnFamily) NewResponse() proto.Message { return &pb.AddColumnResponse{} }

// ModifyColumnFamily replaces an existing column-family descriptor.
type ModifyColumnFamily struct {
	adminMutation
	family *pb.ColumnFamilySchema
}

// NewModifyColumnFamily snapshots a complete descriptor, not an attribute patch.
// Fetch the existing descriptor with GetTableSchema and preserve attributes/configuration
// that should remain unchanged.
func NewModifyColumnFamily(ctx context.Context, table []byte,
	family *pb.ColumnFamilySchema) (*ModifyColumnFamily, error) {
	c, err := NewAddColumnFamily(ctx, table, family)
	if err != nil {
		return nil, err
	}
	return &ModifyColumnFamily{c.adminMutation, c.family}, nil
}

func (c *ModifyColumnFamily) Name() string        { return "ModifyColumn" }
func (c *ModifyColumnFamily) Description() string { return c.Name() }
func (c *ModifyColumnFamily) ToProto() proto.Message {
	return &pb.ModifyColumnRequest{
		TableName: tableNameProto(c.table), ColumnFamilies: c.family,
		NonceGroup: proto.Uint64(adminNonceGroup), Nonce: proto.Uint64(c.nonce),
	}
}
func (c *ModifyColumnFamily) NewResponse() proto.Message { return &pb.ModifyColumnResponse{} }

// TruncateTable removes all rows from a disabled table, then enables it again.
type TruncateTable struct {
	adminMutation
	preserveSplits bool
}

// NewTruncateTable constructs a truncate request. Disable the table explicitly first.
func NewTruncateTable(ctx context.Context, table []byte,
	preserveSplits bool) (*TruncateTable, error) {
	m, err := newAdminMutation(ctx, table)
	if err != nil {
		return nil, err
	}
	return &TruncateTable{m, preserveSplits}, nil
}

func (c *TruncateTable) Name() string        { return "TruncateTable" }
func (c *TruncateTable) Description() string { return c.Name() }
func (c *TruncateTable) ToProto() proto.Message {
	return &pb.TruncateTableRequest{
		TableName: tableNameProto(c.table), PreserveSplits: proto.Bool(c.preserveSplits),
		NonceGroup: proto.Uint64(adminNonceGroup), Nonce: proto.Uint64(c.nonce),
	}
}
func (c *TruncateTable) NewResponse() proto.Message { return &pb.TruncateTableResponse{} }
