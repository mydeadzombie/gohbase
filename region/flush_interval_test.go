// Copyright (C) 2026 The GoHBase Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

//go:build !testing

package region

import (
	"testing"
	"time"
)

func TestFlushIntervalOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *RegionClientOptions
		want time.Duration
	}{
		{"nil", nil, DefaultFlushInterval},
		{"omitted", &RegionClientOptions{}, DefaultFlushInterval},
		{"explicit zero", &RegionClientOptions{FlushIntervalSet: true}, 0},
		{"positive", &RegionClientOptions{FlushInterval: time.Millisecond}, time.Millisecond},
		{"negative", &RegionClientOptions{FlushInterval: -1, FlushIntervalSet: true}, DefaultFlushInterval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("unused", RegionClient, tc.opts).(*client)
			defer c.Close()
			if c.flushInterval != tc.want {
				t.Fatalf("flush interval = %s, want %s", c.flushInterval, tc.want)
			}
		})
	}
}
