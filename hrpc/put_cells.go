// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// PutCell is one version of a column. Timestamp is an explicit HBase timestamp
// (normally milliseconds since epoch). Zero is valid; server-now sentinels are not.
// Different versions of the same family/qualifier may occur in one mutation.
type PutCell struct {
	Family, Qualifier, Value []byte
	Timestamp                uint64
}

// NewPutCells constructs a single-row Put preserving individual cell timestamps.
// Inputs are copied. Timestamp/TimestampUint64 options are rejected: each cell
// already supplies its timestamp. Other mutation options retain their meaning.
// This copies values and timestamps, not cell tags, tombstones or a table snapshot.
func NewPutCells(ctx context.Context, table, key []byte, cells []PutCell,
	options ...func(Call) error) (*Mutate, error) {
	if ctx == nil ||
		len(table) == 0 ||
		len(key) == 0 ||
		len(key) > math.MaxInt16 ||
		len(cells) == 0 {
		return nil, errors.New("context, table, row key (1..32767 bytes) and cells are required")
	}
	type identity struct {
		family, qualifier string
		timestamp         uint64
	}
	seen := make(map[identity]struct{}, len(cells))
	for _, cell := range cells {
		if len(cell.Family) == 0 ||
			len(cell.Family) > math.MaxUint8 ||
			cell.Timestamp >= math.MaxInt64 {
			return nil, errors.New("invalid cell family or explicit timestamp")
		}
		id := identity{string(cell.Family), string(cell.Qualifier), cell.Timestamp}
		if _, ok := seen[id]; ok {
			return nil, errors.New("duplicate family/qualifier/timestamp")
		}
		seen[id] = struct{}{}
	}
	m, err := baseMutate(ctx, bytes.Clone(table), bytes.Clone(key), nil)
	if err != nil {
		return nil, err
	}
	m.mutationType = pb.MutationProto_PUT
	m.cells = clonePutCells(cells)
	sort.Slice(m.cells, func(i, j int) bool {
		a, b := m.cells[i], m.cells[j]
		if cmp := bytes.Compare(a.Family, b.Family); cmp != 0 {
			return cmp < 0
		}
		if cmp := bytes.Compare(a.Qualifier, b.Qualifier); cmp != 0 {
			return cmp < 0
		}
		return a.Timestamp > b.Timestamp
	})
	if err := applyOptions(m, options...); err != nil {
		return nil, err
	}
	return m, nil
}

// PutCells returns a copy of the explicit cells supplied to NewPutCells.
// Map-based mutations return nil; Values returns nil for cell-based puts.
func (m *Mutate) PutCells() []PutCell { return clonePutCells(m.cells) }
func clonePutCells(cells []PutCell) []PutCell {
	if cells == nil {
		return nil
	}
	out := make([]PutCell, len(cells))
	for i, c := range cells {
		out[i] = PutCell{bytes.Clone(c.Family),
			bytes.Clone(c.Qualifier),
			bytes.Clone(c.Value),
			c.Timestamp}
	}
	return out
}
func (m *Mutate) cellsToProto() []*pb.MutationProto_ColumnValue {
	var families []*pb.MutationProto_ColumnValue
	for _, cell := range m.cells {
		if len(families) == 0 || !bytes.Equal(families[len(families)-1].Family, cell.Family) {
			families = append(families, &pb.MutationProto_ColumnValue{Family: cell.Family})
		}
		family := families[len(families)-1]
		family.QualifierValue = append(family.QualifierValue,
			&pb.MutationProto_ColumnValue_QualifierValue{
				Qualifier: cell.Qualifier,
				Value:     cell.Value,
				Timestamp: proto.Uint64(cell.Timestamp),
			})
	}
	return families
}
func (m *Mutate) cellsToCellblocks() ([]byte, int32, uint32) {
	var size int
	for _, cell := range m.cells {
		size += cellblockLen(len(m.key), len(cell.Family), len(cell.Qualifier), len(cell.Value))
	}
	data := make([]byte, 0, size)
	for _, cell := range m.cells {
		data = appendCellblock(m.key,
			string(cell.Family),
			string(cell.Qualifier),
			cell.Value,
			cell.Timestamp,
			putType,
			data)
	}
	return data, int32(len(m.cells)), uint32(len(data))
}
