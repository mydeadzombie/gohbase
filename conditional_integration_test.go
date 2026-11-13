// Copyright (C) 2026 The GoHBase Authors. All rights reserved.
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
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
)

func TestCheckAndMutateConcurrentClients(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := fmt.Sprintf("gohbase_conditional_%d", time.Now().UnixNano())
	if namespace := os.Getenv("HBASE_TEST_NAMESPACE"); namespace != "" && namespace != "default" {
		name = namespace + ":" + name
	}
	admin := gohbase.NewAdminClient(*host)
	defer admin.Close()
	if err := admin.CreateTable(hrpc.NewCreateTable(ctx, []byte(name),
		map[string]map[string]string{"cf": nil})); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		_ = admin.DisableTable(hrpc.NewDisableTable(cleanup, []byte(name)))
		if err := admin.DeleteTable(hrpc.NewDeleteTable(cleanup, []byte(name))); err != nil {
			t.Errorf("cleanup %s: %v", name, err)
		}
	}()
	clients := []gohbase.Client{gohbase.NewClient(*host), gohbase.NewClient(*host)}
	for _, client := range clients {
		defer client.Close()
	}
	put, err := hrpc.NewPutStr(ctx, name, "row",
		map[string]map[string][]byte{"cf": {"state": []byte("initial")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clients[0].Put(put); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		value   string
		applied bool
		err     error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, len(clients))
	for i, client := range clients {
		go func(i int, client gohbase.Client) {
			<-start
			value := fmt.Sprintf("winner-%d", i)
			mutation, err := hrpc.NewPutStr(ctx, name, "row",
				map[string]map[string][]byte{"cf": {"state": []byte(value)}})
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			condition, err := hrpc.NewCheckAndMutate(mutation, "cf", "state", pb.CompareType_EQUAL,
				filter.NewBinaryComparator(filter.NewByteArrayComparable([]byte("initial"))))
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			applied, err := client.CheckAndMutate(condition)
			outcomes <- outcome{value, applied, err}
		}(i, client)
	}
	close(start)
	winners := 0
	var winner string
	for range clients {
		select {
		case result := <-outcomes:
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.applied {
				winners++
				winner = result.value
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if winners != 1 {
		t.Fatalf("got %d successful conditional writes, want 1", winners)
	}

	mutation, err := hrpc.NewPutStr(ctx, name, "row",
		map[string]map[string][]byte{"cf": {"state": []byte("must-not-be-written")}})
	if err != nil {
		t.Fatal(err)
	}
	condition, err := hrpc.NewCheckAndMutate(mutation, "cf", "state", pb.CompareType_EQUAL,
		filter.NewBinaryComparator(filter.NewByteArrayComparable([]byte("initial"))))
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := clients[0].CheckAndMutate(condition); err != nil || applied {
		t.Fatalf("failed condition: applied=%v err=%v", applied, err)
	}
	for _, client := range clients {
		get, err := hrpc.NewGetStr(ctx, name, "row",
			hrpc.Families(map[string][]string{"cf": {"state"}}))
		if err != nil {
			t.Fatal(err)
		}
		result, err := client.Get(get)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Cells) != 1 || string(result.Cells[0].Value) != winner {
			t.Fatalf("stored value does not match winner %q: %v", winner, result)
		}
	}
}
