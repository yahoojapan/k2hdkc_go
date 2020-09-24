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

func testAddSubKeyArgs(d testSubKeyData, t *testing.T) {
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession() = (%v, %v)", session, err)
	}
	defer session.Close()
	if !d.s {
		cmd, _ := k2hdkc.NewAddSubKey(d.key.k, d.skey.k, d.skey.v)
		if ok, err := cmd.Execute(session); !ok {
			t.Errorf("NewAddSubKey(%q, %q, %q).Execute() = (%v, %v)", d.key.k, d.skey.k, d.skey.v, ok, err)
		}
		if ok := cmd.Result().Bool(); !ok {
			t.Errorf("NewAddSubKey(%q, %q, %q).Result().Bool() = %v", d.key.k, d.skey.k, d.skey.v, ok)
		}
		// errno
		if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewAddSubKey(%q).Execute() = %v", d.key.k, ok)
		}
	} else {
		cmd, _ := k2hdkc.NewAddSubKey(string(d.key.k), string(d.skey.k), string(d.skey.v))
		if ok, err := cmd.Execute(session); !ok {
			t.Errorf("NewAddSubKey(%q, %q, %q).Execute() = (%v, %v)", string(d.key.k), string(d.skey.k), string(d.skey.v), ok, err)
		}
		if ok := cmd.Result().Bool(); !ok {
			t.Errorf("NewAddSubKey(%q, %q, %q).Result().Bool() = %v", string(d.key.k), string(d.skey.k), string(d.skey.v), ok)
		}
		// errno
		if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewAddSubKey(%q).Execute() = %v", string(d.key.k), ok)
		}
	}
}

// The actual test functions are in non-_test.go files
// so that they can use cgo (import "C").
// These wrappers are here for gotest to find.

// TestAddSubKey send a binary key and a binary value and get result as a bool.
func testAddSubKey(t *testing.T) {
	// 1. define test data.
	testData := []testSubKeyData{
		{
			d: "string",
			key: kv{
				k: []byte("addsubkeys1_parent_string"),
				v: []byte("p1_string"),
				p: "",
				s: true,
			},
			skey: kv{
				k: []byte("addsubkeys1_sub1_string"),
				v: []byte("s1_string"),
				p: "",
				s: true,
			},
			s: true,
		},
	}
	for i, d := range testData {
		if !d.s {
			if ok, err := clearIfExists(d.key.k); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.key.k, ok, err)
			}
			if ok, err := saveData(d.key.k, d.key.v, d.key.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.key.k, d.key.v, d.key.p, ok, err)
			}
			testAddSubKeyArgs(d, t)
			cnt, keypack, err := callGetSubkeys(d.key.k)
			// make sure cnt is correct.
			if cnt == 0 {
				t.Errorf("callGetSubkeys(%q) should return 1, but returned %v keypack %v err %v", d.key.k, cnt, keypack, err)
			}
			if cnt == 1 && !bytes.Equal(d.skey.k, keypack[i]) {
				t.Errorf("callGetSubkeys(%q) should match d.skey.k %v with keypack %v", d.key.k, d.skey.k, keypack[i])
			}
		} else {
			// TODO string
			if ok, err := clearIfExists(string(d.key.k)); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", string(d.key.k), ok, err)
			}
			if ok, err := saveData(string(d.key.k), string(d.key.v), d.key.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", string(d.key.k), string(d.key.v), d.key.p, ok, err)
			}
			testAddSubKeyArgs(d, t)
			cnt, keypack, err := callGetSubkeysString(string(d.key.k))
			// make sure cnt is correct.
			if cnt == 0 {
				t.Errorf("callGetSubkeys(%q) should return 1, but returned %v keypack %v err %v", string(d.key.k), cnt, keypack, err)
			}
			if cnt == 1 && string(d.skey.k) != string(keypack[i]) {
				t.Errorf("callGetSubkeys(%q) should match d.skey.k %v with keypack %v", string(d.key.k), string(d.skey.k), string(keypack[i]))
			}
		}
	}
}

// TestAddSubKeyKeyEmpty send a empty binary key.
func testAddSubKeyTypeStringEmpty(t *testing.T) {

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
	cmd, err := k2hdkc.NewAddSubKey(key, nil, nil)
	if err == nil {
		t.Errorf("NewAddSubKey(%q, nil, nil) = (%v, %v)", key, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewAddSubKey(%q, nil, nil) = (%v, %q), want %q", key, cmd, err, wantError)
		}
	}
}

// TestAddSubKeyKeyTypeUnknown send a invalid type key.
func testAddSubKeyKeyTypeUnknown(t *testing.T) {

	key := uint8(255)

	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession() = (%v, %v)", session, err)
	}
	defer session.Close()

	// execute a get command.
	cmd, err := k2hdkc.NewAddSubKey(key, nil, nil)
	if err == nil {
		t.Errorf("NewAddSubKey(%q) = (cmd, %q), want not nil", key, err)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewAddSubKey(%q) = (%v, %v), want %v", key, cmd, err, wantError)
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
