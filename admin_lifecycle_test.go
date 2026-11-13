// Copyright (C) 2026  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/tsuna/gohbase/hrpc"
	mockRegion "github.com/tsuna/gohbase/test/mock/region"
	"go.uber.org/mock/gomock"
)

func TestAdminCloseBeforeUse(t *testing.T) {
	// Construction must not need a live cluster, and closing twice must be safe.
	admin := NewAdminClient("unused:2181")
	admin.Close()
	admin.Close()
	request := hrpc.NewDisableTable(context.Background(), []byte("table"))
	if err := admin.DisableTable(request); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("operation after close returned %v", err)
	}
}

func TestAdminCloseOwnsMasterConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	rc := mockRegion.NewMockRegionClient(ctrl)
	rc.EXPECT().Close().Times(1)
	c := newAdminClient("unused:2181").(*client)
	c.publishAdminClient(rc)
	c.Close()
	c.Close()
	if c.adminRegionInfo.Client() != nil {
		t.Fatal("closed connection retained")
	}
	if c.adminRegionInfo.Context().Err() == nil {
		t.Fatal("master dial context not canceled")
	}
}

func TestAdminPublishAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	rc := mockRegion.NewMockRegionClient(ctrl)
	rc.EXPECT().Close().Times(1)
	c := newAdminClient("unused:2181").(*client)
	c.Close()
	c.publishAdminClient(rc)
	if c.adminRegionInfo.Client() != nil {
		t.Fatal("late master connection published")
	}
}

func TestAdminPublishRacesClose(t *testing.T) {
	for range 100 {
		ctrl := gomock.NewController(t)
		rc := mockRegion.NewMockRegionClient(ctrl)
		rc.EXPECT().Close().Times(1)
		c := newAdminClient("unused:2181").(*client)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.publishAdminClient(rc) }()
		go func() { defer wg.Done(); c.Close() }()
		wg.Wait()
		if c.adminRegionInfo.Client() != nil {
			t.Fatal("connection retained after shutdown")
		}
	}
}

func TestAdminReplacementClosesOldConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	old := mockRegion.NewMockRegionClient(ctrl)
	next := mockRegion.NewMockRegionClient(ctrl)
	old.EXPECT().Close().Times(1)
	next.EXPECT().Close().Times(1)
	c := newAdminClient("unused:2181").(*client)
	c.publishAdminClient(old)
	c.publishAdminClient(next)
	c.Close()
}
