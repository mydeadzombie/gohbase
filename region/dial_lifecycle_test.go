// Copyright (C) 2026 The GoHBase Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package region

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCloseBeforeDialDoesNotConnect(t *testing.T) {
	c := NewClient("unused", RegionClient, &RegionClientOptions{
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			t.Error("dialer called after Close")
			return nil, errors.New("unexpected dial")
		},
	})
	c.Close()
	if err := c.Dial(context.Background()); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("got %v, want ErrClientClosed", err)
	}
}

func TestCloseDuringDialClosesLateConnection(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	conn, peer := net.Pipe()
	defer peer.Close()
	defer conn.Close()
	c := NewClient("unused", RegionClient, &RegionClientOptions{
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			close(started)
			<-release
			return conn, nil
		},
	})
	defer c.Close()
	go func() { done <- c.Dial(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("dial did not start")
	}
	c.Close()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("unexpected result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial did not finish after shutdown")
	}
	// No handshake should have been sent; the returned socket must be closed.
	if err := conn.SetWriteDeadline(time.Now()); err == nil {
		t.Fatal("late connection remains open")
	}
}
