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

// TestSet holds normal test cases for Set.
func testSet(t *testing.T) {

	// 1. define test data.
	var testData = []kv{
		{d: "binary", k: []byte("set1"), v: []byte("v1"), p: "", e: 0, r: false, s: false},
		{d: "string", k: []byte("set2"), v: []byte("v2"), p: "", e: 0, r: false, s: true},
		{d: "encrpt", k: []byte("set3"), v: []byte("v3"), p: "pass", e: 0, r: false, s: false},
		{d: "expire", k: []byte("set4"), v: []byte("v4"), p: "", e: 3, r: false, s: false},
		{d: "removesubkeys", k: []byte("set5"), v: []byte("v5"), p: "", e: 0, r: true, s: false},
	}
	testSetSubKey := []byte("SetSubKey") // slice is mutable so I couldn't define it as const.
	testSetSubVal := []byte("SetSubVal")
	for _, d := range testData {
		if !d.s {
			if ok, err := clearIfExists(d.k); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.k, ok, err)
			}
			// make a key with a subkey if rmsubkeys is true.
			if d.r {
				// make a key and a subkey.
				if ok, err := saveData(d.k, d.v, d.p); !ok {
					t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.k, d.v, d.p, ok, err)
				}
				if ok, err := saveData(testSetSubKey, testSetSubVal, ""); !ok {
					t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.k, d.v, d.p, ok, err)
				}
				// make a parent key to hold a child key as a subkey.
				subkeys := make([][]byte, 1)
				subkeys[0] = testSetSubKey
				if ok, err := callSetSubkeys(d.k, subkeys); !ok {
					t.Errorf("callSetSubkeys(%q, %q) = (%v, %v)", d.k, subkeys, ok, err)
				}
			}
		} else {
			if ok, err := clearIfExists(string(d.k)); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", string(d.k), ok, err)
			}
			// make a key with a subkey if rmsubkeys is true.
			if d.r {
				// make a key and a subkey.
				if ok, err := saveData(string(d.k), string(d.v), d.p); !ok {
					t.Errorf("saveData(%q, %q, %q) = (%v, %v)", string(d.k), string(d.v), d.p, ok, err)
				}
				if ok, err := saveData(string(testSetSubKey), string(testSetSubVal), ""); !ok {
					t.Errorf("saveData(%q, %q, %q) = (%v, %v)", string(d.k), string(d.v), d.p, ok, err)
				}
				// make a parent key to hold a child key as a subkey.
				subkeys := make([]string, 1)
				subkeys[0] = string(testSetSubKey)
				if ok, err := callSetSubkeys(string(d.k), subkeys); !ok {
					t.Errorf("callSetSubkeys(%q, %q) = (%v, %v)", string(d.k), subkeys, ok, err)
				}
			}
		}

		// exec tests
		testSetArgs(d, t)

		// check results.
		if ok, err := callGetKey(d); err != nil {
			t.Errorf("callGetKey(%q) = (%v, %v)", d.k, ok, err)
		}
		// check if no subkey exists if rmsubkeys is true.
		if d.r {
			// call k2hdkc_pm_get_subkeys.
			if !d.s {
				if cnt, keypack, err := callGetSubkeys(d.k); cnt != 0 {
					t.Errorf("callGetSubkeys(%q) = (%v, %v, %v)", d.k, cnt, keypack, err)
				}
			} else {
				if cnt, keypack, err := callGetSubkeysString(string(d.k)); cnt != 0 {
					t.Errorf("callGetSubkeys(%q) = (%v, %v, %v)", string(d.k), cnt, keypack, err)
				}
			}
		}
	}
}
func testSetArgs(d kv, t *testing.T) {

	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(client) = (%v, %v)", session, err)
	}
	defer session.Close()

	// set data
	var cmd *k2hdkc.Set
	if d.s {
		cmd, err = k2hdkc.NewSet(string(d.k), string(d.v))
		if err != nil {
			t.Errorf("NewSet(%q, %q) = (%v, %v)", string(d.k), string(d.v), cmd, err)
		}
	} else {
		cmd, err = k2hdkc.NewSet(d.k, d.v)
		if err != nil {
			t.Errorf("NewSet(%q, %q) = (%v, %v)", d.k, d.v, cmd, err)
		}
	}
	if d.p != "" {
		cmd.SetEncPass(d.p)
	}
	if d.e != 0 {
		cmd.SetExpire(d.e)
	}
	if d.r {
		cmd.SetRmSubKeyList(true)
	}
	if ok, err := cmd.Execute(session); !ok {
		t.Errorf("NewSet(%q, %q).Execute() = (%v, %v)", d.k, d.v, ok, err)
	}
	if ok := cmd.Result().Bool(); !ok {
		t.Errorf("NewSet(%q, %q).Execute() = %v", d.k, d.v, ok)
	}
	if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewSet(%q, %q).Execute() = %v", d.k, d.v, ok)
	}
}

// TestSetTypeUnknown simulates sending an invalid format key data.
func testSetTypeUnknown(t *testing.T) {

	// 1. define test data.
	key := uint8(255)
	want := "want"

	// 2. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(client) = %v", err)
	}
	defer session.Close()

	// 3. execute a set command.
	cmd, err := k2hdkc.NewSet(key, want)
	if err == nil {
		t.Errorf("NewSet(%q, %q) = (%v, %v), want not nil", key, want, cmd, err)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewSet(%q, %q) = (%v, %q), want %q", key, want, cmd, err, wantError)
		}
	}
}

// TestSetTypeEmpty simulates sending an empty key data.
func testSetTypeEmpty(t *testing.T) {
	// 1. define test data.
	key := ""
	want := "want"

	// 2. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(%s) = (%v, %v)", client, session, err)
	}
	defer session.Close()

	// 3. execute a set command.
	cmd, err := k2hdkc.NewSet(key, want)
	if err == nil {
		t.Errorf("NewSet(%q, %q) = (%v, %v), want not nil", key, want, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewSet(%q, %q) = (%v, %v), want %q", key, want, cmd, err, wantError)
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
