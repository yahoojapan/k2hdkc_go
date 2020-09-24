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
	"bytes"
	"testing"

	"github.com/yahoojapan/k2hdkc_go/k2hdkc"
)

// TestGet holds normal test cases for Get.
func testGet(t *testing.T) {

	// 1. define test data.
	var testData = []kv{
		{d: "binary", k: []byte("get1"), v: []byte("v1"), p: "", e: 0, r: false, s: false},
		{d: "string", k: []byte("get2"), v: []byte("v2"), p: "", e: 0, r: false, s: true},
		{d: "encrpt", k: []byte("get3"), v: []byte("v3"), p: "pass", e: 0, r: false, s: false},
	}
	// 2. exec tests
	for _, d := range testData {
		if d.s {
			if ok, err := clearIfExists(string(d.k)); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.k, ok, err)
			}
		} else {
			if ok, err := clearIfExists(d.k); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.k, ok, err)
			}
		}
		if d.s {
			if ok, err := saveData(string(d.k), string(d.v), d.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.k, d.v, d.p, ok, err)
			}
		} else {
			if ok, err := saveData(d.k, d.v, d.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.k, d.v, d.p, ok, err)
			}
		}
		testGetArgs(d, t)
	}
}

func testGetArgs(d kv, t *testing.T) {
	// 2. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(client) = %v", err)
	}
	defer session.Close()

	// 3. execute a get command.
	var cmd *k2hdkc.Get
	if d.s {
		cmd, err = k2hdkc.NewGet(string(d.k))
		if err != nil {
			t.Errorf("NewGet(%q) = (%v, %v)", string(d.k), cmd, err)
		}
	} else {
		cmd, err = k2hdkc.NewGet(d.k)
		if err != nil {
			t.Errorf("NewGet(%q) = (%v, %v)", d.k, cmd, err)
		}
	}
	if d.p != "" {
		cmd.SetEncPass(d.p)
	}
	if ok, err := cmd.Execute(session); !ok {
		t.Errorf("NewGet(%q).Execute() = (%v, %v)", d.k, ok, err)
	}
	if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewGet(%q).Execute() = %v", d.k, ok)
	}
	if d.s {
		text := cmd.Result().String()
		if text != string(d.v) {
			t.Errorf("Result().String() = %v, want %v", text, string(d.v))
		}
	} else {
		bin := cmd.Result().Bytes()
		if !bytes.Equal(bin, []byte(d.v)) {
			t.Errorf("Result().Bytes() = %v, want %v", bin, d.v)
		}
	}
}

// TestGetKeyEmpty send a empty binary key.
func testGetTypeStringEmpty(t *testing.T) {

	key := ""

	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(%v) = (%v, %v)", client, session, err)
	}
	defer session.Close()

	// execute a get command.
	cmd, err := k2hdkc.NewGet(key)
	if err == nil {
		t.Errorf("NewGet(%v) = (%v, %v)", key, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewGet(%q) = (%v, %q), want %q", key, cmd, err, wantError)
		}
	}
}

// TestGetKeyTypeUnknown send a invalid type key.
func testGetKeyTypeUnknown(t *testing.T) {

	key := uint8(255)

	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	// execute a get command.
	cmd, err := k2hdkc.NewGet(key)
	if err == nil {
		t.Errorf("NewGet(%q) = (%v, %v), want not nil", key, cmd, err)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewGet(%q) = (%v, %v), want %v", key, cmd, err, wantError)
		}
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
