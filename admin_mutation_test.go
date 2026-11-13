// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	mockRegion "github.com/tsuna/gohbase/test/mock/region"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func TestAdminMutationProcedure(t *testing.T) {
	for _, operation := range []string{"AddColumn", "ModifyColumn", "TruncateTable"} {
		for _, scenario := range []string{"success", "missing_id", "rpc_error", "wrong_response",
			"procedure_error", "canceled", "wrong_procedure_response"} {
			t.Run(operation+"/"+scenario, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				rc := mockRegion.NewMockRegionClient(ctrl)
				c := newMockClient(nil)
				c.clientType = region.MasterClient
				c.adminRegionInfo = region.NewInfo(0, nil, nil, nil, nil, nil)
				c.adminRegionInfo.SetClient(rc)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				polls := 0
				rc.EXPECT().QueueRPC(gomock.Any()).Do(func(call hrpc.Call) {
					if call.Name() == operation {
						if scenario == "rpc_error" {
							call.ResultChan() <- hrpc.RPCResult{Error: errors.New("admin rejected")}
							return
						}
						var id *uint64
						if scenario != "missing_id" {
							id = proto.Uint64(42)
						}
						var msg proto.Message
						switch operation {
						case "AddColumn":
							msg = &pb.AddColumnResponse{ProcId: id}
						case "ModifyColumn":
							msg = &pb.ModifyColumnResponse{ProcId: id}
						case "TruncateTable":
							msg = &pb.TruncateTableResponse{ProcId: id}
						}
						if scenario == "wrong_response" {
							msg = &pb.GetResponse{}
						}
						call.ResultChan() <- hrpc.RPCResult{Msg: msg}
						return
					}
					if call.Name() != "getProcedureResult" {
						t.Errorf("unexpected RPC %s", call.Name())
					}
					if call.ToProto().(*pb.GetProcedureResultRequest).GetProcId() != 42 {
						t.Error("wrong procedure ID")
					}
					polls++
					res := &pb.GetProcedureResultResponse{
						State: pb.GetProcedureResultResponse_FINISHED.Enum(),
					}
					switch scenario {
					case "success":
						if polls == 1 {
							res.State = pb.GetProcedureResultResponse_RUNNING.Enum()
						}
					case "procedure_error":
						res.Exception = &pb.ForeignExceptionMessage{
							GenericException: &pb.GenericExceptionMessage{
								ClassName: proto.String("Failure"),
								Message:   proto.String("procedure failed"),
							}}
					case "canceled":
						res.State = pb.GetProcedureResultResponse_RUNNING.Enum()
						cancel()
					case "wrong_procedure_response":
						call.ResultChan() <- hrpc.RPCResult{Msg: &pb.GetResponse{}}
						return
					}
					call.ResultChan() <- hrpc.RPCResult{Msg: res}
				}).AnyTimes()
				family := &pb.ColumnFamilySchema{Name: []byte("cf")}
				var err error
				switch operation {
				case "AddColumn":
					req, e := hrpc.NewAddColumnFamily(ctx, []byte("ns:table"), family)
					if e != nil {
						t.Fatal(e)
					}
					err = c.AddColumnFamily(req)
				case "ModifyColumn":
					req, e := hrpc.NewModifyColumnFamily(ctx, []byte("ns:table"), family)
					if e != nil {
						t.Fatal(e)
					}
					err = c.ModifyColumnFamily(req)
				case "TruncateTable":
					req, e := hrpc.NewTruncateTable(ctx, []byte("ns:table"), true)
					if e != nil {
						t.Fatal(e)
					}
					err = c.TruncateTable(req)
				}
				if scenario == "success" {
					if err != nil || polls != 2 {
						t.Fatalf("err=%v polls=%d", err, polls)
					}
				} else if err == nil {
					t.Fatal("expected failure")
				}
				if scenario == "canceled" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if scenario == "procedure_error" &&
					!strings.Contains(err.Error(), "procedure failed") {
					t.Fatal(err)
				}
			})
		}
	}
}
