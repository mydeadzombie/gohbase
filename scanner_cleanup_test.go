// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"context"
	"testing"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

type cleanupRPC func(hrpc.Call) (proto.Message, error)

func (f cleanupRPC) SendRPC(call hrpc.Call) (proto.Message, error) { return f(call) }

func TestScannerCleanupHasIndependentDeadline(t *testing.T) {
	original, cancel := context.WithCancel(context.Background())
	cancel()
	scan, err := hrpc.NewScanStr(original, "table")
	if err != nil {
		t.Fatal(err)
	}
	calls := make(chan hrpc.Call, 1)
	release := make(chan struct{})
	defer close(release)
	s := &scanner{
		rpc: scan, curRegionScannerID: 42,
		RPCClient: cleanupRPC(func(call hrpc.Call) (proto.Message, error) {
			calls <- call
			<-release
			return &pb.ScanResponse{}, nil
		}),
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case call := <-calls:
		if call.Context().Err() != nil {
			t.Fatal("cleanup inherited canceled scan context")
		}
		deadline, ok := call.Context().Deadline()
		if !ok || time.Until(deadline) > scannerCloseTimeout {
			t.Fatal("cleanup must have bounded lifetime")
		}
		if !call.(*hrpc.Scan).IsClosing() {
			t.Fatal("not a close request")
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup request was not sent")
	}
}
