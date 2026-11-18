// Copyright (C) 2026 The GoHBase Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"context"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/test/mock"
	"go.uber.org/mock/gomock"
)

func zeroFlushClient(t *testing.T) (*client, *int) {
	t.Helper()
	conn := mock.NewMockConn(gomock.NewController(t))
	writes := new(int)
	conn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
		*writes++
		return len(b), nil
	}).AnyTimes()
	conn.EXPECT().SetReadDeadline(gomock.Any()).AnyTimes()
	conn.EXPECT().Close().Return(nil)
	c := &client{
		conn: conn, rpcs: make(chan []hrpc.Call), done: make(chan struct{}),
		sent: make(map[uint32]hrpc.Call), rpcQueueSize: 10,
		flushInterval: 0, logger: slog.Default(),
	}
	t.Cleanup(func() { c.Close(); synctest.Wait() })
	return c, writes
}

func zeroFlushGet(t *testing.T, ctx context.Context) *hrpc.Get {
	t.Helper()
	g, err := hrpc.NewGetStr(ctx, "table", "row")
	if err != nil {
		t.Fatal(err)
	}
	g.SetRegion(reg0)
	return g
}

func flushCount(t *testing.T, reason string) float64 {
	t.Helper()
	var metric dto.Metric
	if err := flushReasonCount.WithLabelValues(reason).Write(&metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func expectClosedResult(t *testing.T, call hrpc.Call) {
	t.Helper()
	select {
	case result := <-call.ResultChan():
		if result.Error != ErrClientClosed {
			t.Fatalf("result error = %v, want ErrClientClosed", result.Error)
		}
	default:
		t.Fatal("request lost its shutdown result")
	}
	select {
	case <-call.ResultChan():
		t.Fatal("duplicate shutdown result")
	default:
	}
}

func TestZeroFlushSendsWithoutTimerAndBlocksWhenIdle(t *testing.T) {
	for _, tc := range []struct {
		name   string
		calls  int
		reason string
	}{
		{"partial", 1, "immediate"},
		{"full", 10, "queue full"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, writes := zeroFlushClient(t)
				go c.processRPCs()
				synctest.Wait() // Must block on the empty queue, not spin or send.
				if *writes != 0 {
					t.Fatal("idle client wrote a request")
				}
				beforeImmediate := flushCount(t, "immediate")
				beforeFull := flushCount(t, "queue full")
				calls := make([]hrpc.Call, tc.calls)
				for i := range calls {
					calls[i] = zeroFlushGet(t, t.Context())
				}
				start := time.Now()
				c.QueueBatch(t.Context(), calls)
				synctest.Wait()
				if *writes != 1 || !time.Now().Equal(start) {
					t.Fatalf("writes=%d elapsed=%s; wanted one write without timer advancement",
						*writes, time.Since(start))
				}
				immediate := flushCount(t, "immediate") - beforeImmediate
				full := flushCount(t, "queue full") - beforeFull
				if (tc.reason == "immediate" && (immediate != 1 || full != 0)) ||
					(tc.reason == "queue full" && (immediate != 0 || full != 1)) {
					t.Fatalf("flush counters: immediate=%v full=%v", immediate, full)
				}
				time.Sleep(time.Second) // Virtual time; idle must not emit more RPCs.
				synctest.Wait()
				if *writes != 1 {
					t.Fatal("idle client emitted another RPC")
				}
				c.Close()
				synctest.Wait()
				for _, call := range calls {
					expectClosedResult(t, call)
				}
				if len(c.sent) != 0 {
					t.Fatal("in-flight calls retained after shutdown")
				}
			})
		})
	}
}

func TestZeroFlushCanceledQueueReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, writes := zeroFlushClient(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		call := zeroFlushGet(t, ctx)
		returned := false
		// No worker yet: admission is blocked until cancellation.
		go func() { c.QueueRPC(call); returned = true }()
		synctest.Wait()
		if returned {
			t.Fatal("queue admission did not block")
		}
		cancel()
		synctest.Wait()
		if !returned {
			t.Fatal("canceled queue admission is stuck")
		}
		go c.processRPCs()
		synctest.Wait()
		if *writes != 0 {
			t.Fatal("canceled request reached the connection")
		}
		// QueueRPC cancellation is signaled by the context, not ResultChan.
	})
}

func TestZeroFlushShutdownReleasesQueuedCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, writes := zeroFlushClient(t)
		calls := make([]hrpc.Call, 20)
		for i := range calls {
			calls[i] = zeroFlushGet(t, t.Context())
			go c.QueueRPC(calls[i])
		}
		synctest.Wait() // All producers are waiting for queue admission.
		c.Close()
		synctest.Wait()
		for _, call := range calls {
			expectClosedResult(t, call)
		}
		go c.processRPCs()
		synctest.Wait() // A worker started after Close must also exit.
		if *writes != 0 {
			t.Fatal("closed client wrote a request")
		}
	})
}
