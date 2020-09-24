//
// k2hdkc_go
//
// Copyright 2018 Yahoo Japan Corporation.
//
// Go driver for k2hdkc that is a highly available and scalable distributed
// KVS clustering system. For k2hdkc, see
// https://github.com/yahoojapan/k2hdkc for the details.
//
// For the full copyright and license information, please view
// the license file that was distributed with this source code.
//
// AUTHOR:   Hirotaka Wakabayashi
// CREATE:   Fri, 14 Sep 2018
// REVISION:
//

package k2hdkctest

import (
	// #cgo CFLAGS: -g -Wall -O2 -Wall -Wextra -Wno-unused-variable -Wno-unused-parameter -I. -I/usr/include/k2hdkc
	// #cgo LDFLAGS: -L/usr/lib -lk2hdkc
	// #include <stdlib.h>
	// #include "k2hdkc.h"
	"C"
)

import (
	"fmt"
	"os"
	"testing"

	"github.com/yahoojapan/k2hdkc_go/k2hdkc"
)

func testClient(t *testing.T) {
	c := k2hdkc.NewClient("../cluster/slave.yaml", 100)
	if c == nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 100) returned nil")
	}
	c.Close()
}

func testClientCreateSession(t *testing.T) {
	c := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	if c == nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031) returned nil")
	}
	s, err := c.CreateSession()
	if err != nil {
		t.Errorf("c.CreateSession() returned %v", err)
	}
	if s != nil {
		s.Close()
	}
	defer c.Close()
}

func testClientCreateSessionError(t *testing.T) {
	c := k2hdkc.NewClient("notfound.yaml", 100)
	if c == nil {
		t.Errorf("NewClient(notfound.yaml, 100) = nil, want not nil")
	}
	defer c.Close()
	wantError := "no notfound.yaml exists"
	s, err := c.CreateSession()
	if err == nil || err.Error() != wantError {
		t.Errorf("c.CreateSession() returned err %q want %v", err, wantError)
	}
	if s != nil {
		s.Close()
	}
}

func testClientSetMethods(t *testing.T) {
	c := &k2hdkc.Client{}
	c.SetChmpxFile("../cluster/slave.yaml")
	c.SetCtlPort(8031)
	c.SetAutoRejoin(true)
	c.SetAutoRejoinRetry(true)
	c.SetCleanup(true)
	log := k2hdkc.K2hLogInstance()
	c.SetLogger(log)
	c.SetLogFile("test.log")
	c.SetLogSeverity(k2hdkc.SeverityError)
	c.SetLibLogSeverity(k2hdkc.LibK2hdkc)
	c.Close()
}

func testClientSetAndGet(t *testing.T) {
	c := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer c.Close()
	if r, err := c.Set("key", "value"); r == nil || err != nil {
		t.Errorf("client.Set(key, value) returned r %v err %v", r, err)
	}
	r, err := c.Get("key")
	if r == nil || err != nil {
		t.Errorf("client.Get(key) returned r %v err %v", r, err)
	}
	if r.String() != "value" {
		t.Errorf("client.Get(key) returned r.val %v want value err %s", r.String(), err)
	}
}

func testClientSetSubKeysAndGetSubKeys(t *testing.T) {
	c := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer c.Close()
	// 1. add a child.
	if r, err := c.Set("child", "child_value"); r == nil || err != nil {
		t.Errorf("client.Set(key, value) returned r %v err %v", r, err)
	}
	r, err := c.Get("child")
	if r == nil || err != nil {
		t.Errorf("client.Get(key) returned r %v err %v", r, err)
	}
	if r.String() != "child_value" {
		t.Errorf("client.Get(key) returned r.val %v want child_value err %s", r.String(), err)
	}
	// 2. add a parent.
	if r, err := c.Set("parent", "parent_value"); r == nil || err != nil {
		t.Errorf("client.Set(key, value) returned r %v err %v", r, err)
	}
	r, err = c.Get("parent")
	if r == nil || err != nil {
		t.Errorf("client.Get(key) returned r %v err %v", r, err)
	}
	if r.String() != "parent_value" {
		t.Errorf("client.Get(key) returned r.val %v want parent_value err %s", r.String(), err)
	}
	// 3. add the child as a subkey of the parent.
	skeys := []string{"child"}
	if r, err := c.SetSubKeys("parent", skeys); r == nil || err != nil {
		t.Errorf("client.SetSubKeys(key, value) returned r %v err %v", r, err)
	}
	r2, err := c.GetSubKeys("parent")
	if r2 == nil || err != nil {
		t.Errorf("client.GetSubKeys(key) returned r %v err %v", r2, err)
	}
	slice := r2.String()
	if slice == nil || len(slice) != 1 || slice[0] != "child" {
		t.Errorf("GetSubKeysResult.String() returned %v", slice)
	}
}

// ExampleClient is a example using client.go.
func ExampleClient() {
	c := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer c.Close()
	// 1. save a child key.
	if r, err := c.Set("child", "child_value"); r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.Set(key, value) returned r %v err %v\n", r, err)
		return
	}
	r, err := c.Get("child")
	if r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.Get(key) returned r %v err %v\n", r, err)
		return
	}
	if len(string(r.Bytes())) == 0 || string(r.Bytes()) != "child_value" {
		fmt.Fprintf(os.Stderr, "client.Get(key) returned r.val %v want child_value err %s\n", string(r.Bytes()), err)
		return
	}
	// 2. save a parent key.
	if r, err := c.Set("parent", "parent_value"); r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.Set(key, value) returned r %v err %v", r, err)
		return
	}
	r, err = c.Get("parent")
	if r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.Get(key) returned r %v err %v", r, err)
		return
	}
	if len(string(r.Bytes())) == 0 || string(r.Bytes()) != "parent_value" {
		fmt.Fprintf(os.Stderr, "client.Get(key) returned r.val %v want parent_value err %s", string(r.Bytes()), err)
		return
	}
	// 3. save the child key as a subkey of the parent key.
	skeys := []string{"child"}
	if r, err := c.SetSubKeys("parent", skeys); r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.SetSubKeys(key, value) returned r %v err %v", r, err)
		return
	}
	r2, err := c.GetSubKeys("parent")
	if r2 == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.GetSubKeys(key) returned r %v err %v", r2, err)
		return
	}
	r3 := r2.String()
	if r3 == nil || len(r3) != 1 || r3[0] != "child" {
		fmt.Fprintf(os.Stderr, "GetSubKeysResult.String() returned %v", r3)
		return
	}
	// fmt.Println(r3[0])
	// Output:
	// child
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
