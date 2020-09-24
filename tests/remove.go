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
	"testing"

	"github.com/yahoojapan/k2hdkc_go/k2hdkc"
)

// TestRemoveTypeByte send a binary key and get result.
func testRemoveTypeByte(t *testing.T) {

	// define test data.
	var testData = []kv{
		{d: "binary", k: []byte("remove1"), v: []byte("v1"), p: "", e: 0, r: false, s: false},
		{d: "string", k: []byte("remove2"), v: []byte("v2"), p: "", e: 0, r: false, s: true},
	}

	for _, d := range testData {
		if !d.s {
			if ok, err := clearIfExists(d.k); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.k, ok, err)
			}
			if ok, err := saveData(d.k, d.v, d.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.k, d.v, d.p, ok, err)
			}
		} else {
			if ok, err := clearIfExists(string(d.k)); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.k, ok, err)
			}
			if ok, err := saveData(string(d.k), string(d.v), d.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.k, d.v, d.p, ok, err)
			}
		}

		// exec tests
		testRemoveArgs(d, t)

		// check the result.
		ok, val, err := getKey(d)
		if !ok {
			t.Errorf("getKey(%v) = (%v, %v, %v), want ok is true", d, ok, val, err)
		}
		if len(val) != 0 {
			t.Errorf("getKey(%v) = (%v, %v, %v), want len(val) == 0", d, ok, val, err)
		}
	}
}

func testRemoveArgs(d kv, t *testing.T) {
	// 2. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	// 3. execute a remove command.
	var cmd *k2hdkc.Remove
	if d.s {
		cmd, err = k2hdkc.NewRemove(string(d.k))
	} else {
		cmd, err = k2hdkc.NewRemove(d.k)
	}
	if err != nil {
		t.Errorf("NewRemove(TestRemove) returned %v", err)
	}
	_, err = cmd.Execute(session)
	if err != nil {
		t.Errorf("cmd.Execute(session) = %v", err)
	}
	if ok, err := cmd.Execute(session); !ok {
		t.Errorf("NewRetCommand(%q).Execute() = (%v, %v)", d.k, ok, err)
	}
	if ok := cmd.Result().Bool(); !ok {
		t.Errorf("cmd.Result().Bool() = %v, want true", ok)
	}
	if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewRetCommand(%q).Execute() = %v", d.k, ok)
	}
}

// TestRemoveKeyEmpty send a empty binary key.
func testRemoveTypeStringEmpty(t *testing.T) {
	key := ""

	// 2. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(%v) = (%v, %v)", client, session, err)
	}
	defer session.Close()

	// execute a get command.
	cmd, err := k2hdkc.NewRemove(key)
	if err == nil {
		t.Errorf("NewRemove(%v) = (%v, %v)", key, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewRemove(%q) = (%v, %q), want %q", key, cmd, err, wantError)
		}
	}
}

// TestRemoveKeyTypeUnknown send a invalid type key.
func testRemoveKeyTypeUnknown(t *testing.T) {

	key := uint8(255)

	// 1. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	// execute a remove command.
	cmd, err := k2hdkc.NewRemove(key)
	if err == nil {
		t.Errorf("NewRemove(%q) = (%v, %v), want not nil", key, cmd, err)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewRemove(%q) = (%v, %v), want %v", key, cmd, err, wantError)
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
