// Copyright (C) 2026 The GoHBase Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"log/slog"
	"testing"

	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/region"
	mockRegion "github.com/tsuna/gohbase/test/mock/region"
	"go.uber.org/mock/gomock"
)

func TestCacheEvictionClosesClientOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	rc := mockRegion.NewMockRegionClient(ctrl)
	cache := clientRegionCache{
		logger: slog.Default(), regions: make(map[hrpc.RegionClient]map[hrpc.RegionInfo]struct{}),
	}
	reg := region.NewInfo(1, nil, []byte("table"), []byte("table,,1"), nil, nil)
	rc.EXPECT().String().Return("test client").AnyTimes()
	rc.EXPECT().Close().Do(func() {
		// Cleanup must be outside the lock, and removal must precede Close.
		cache.m.Lock()
		defer cache.m.Unlock()
		if _, exists := cache.regions[rc]; exists {
			t.Error("client still cached during Close")
		}
	}).Times(1)
	cache.put("server", reg, func() hrpc.RegionClient { return rc })
	reg.SetClient(rc)
	if regions := cache.clientDown(rc); len(regions) != 1 {
		t.Fatal("region list lost")
	}
	cache.clientDown(rc)
	cache.closeAll()
}

func TestCacheShutdownRejectsLateInsertion(t *testing.T) {
	ctrl := gomock.NewController(t)
	rc := mockRegion.NewMockRegionClient(ctrl)
	cache := clientRegionCache{
		logger: slog.Default(), regions: make(map[hrpc.RegionClient]map[hrpc.RegionInfo]struct{}),
	}
	reg := region.NewInfo(1, nil, []byte("table"), []byte("table,,1"), nil, nil)
	rc.EXPECT().String().Return("test client").AnyTimes()
	rc.EXPECT().Close().Times(1)
	cache.put("server", reg, func() hrpc.RegionClient { return rc })
	reg.SetClient(rc)
	cache.closeAll()
	cache.closeAll()
	if reg.Context().Err() == nil || reg.Client() != nil {
		t.Fatal("region not released")
	}
	if len(cache.regions) != 0 {
		t.Fatal("cache retained client references")
	}
	got := cache.put("server", reg, func() hrpc.RegionClient {
		t.Error("created client after shutdown")
		return rc
	})
	if got != nil {
		t.Fatal("accepted client after shutdown")
	}
}
