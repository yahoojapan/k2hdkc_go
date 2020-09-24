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

// TestRemoveSubKeyTypeByte send a binary key and a binary value and get result as a bool.
func testRemoveSubKey(t *testing.T) {

	// 1. define test data.
	testData := []testSubKeysData{
		{
			d: "binary",
			key: kv{
				k: []byte("removesubkeys1_parent"),
				v: []byte("p1"),
				p: "",
			},
			keys: []kv{
				{
					k: []byte("removesubkeys1_sub1"),
					v: []byte("s1"),
					p: "",
				},
				{
					k: []byte("removesubkeys1_sub2"),
					v: []byte("s2"),
					p: "",
				},
			},
		},
	}
	for _, d := range testData {
		if ok, err := clearIfExists(d.key.k); !ok {
			t.Errorf("clearIfExists(%q) = (%v, %v)", d.key.k, ok, err)
		}
		// 2.1 a key
		if ok, err := saveData(d.key.k, d.key.v, d.key.p); !ok {
			t.Errorf("saveData(%q, %q, %q) = (%v, %v)", d.key.k, d.key.v, d.key.p, ok, err)
		}
		// 2.2 keys of a subkey
		for _, key := range d.keys {
			if ok, err := saveData(key.k, key.v, key.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", key.k, key.v, key.p, ok, err)
			}
		}
		testRemoveSubKeyArgs(d, t)
		cnt, keypack, err := callGetSubkeys(d.key.k)
		if cnt != 0 {
			t.Errorf("callGetSubkeys(%q) = (%v, %v, %v), want cnt == 0", d.key.k, cnt, keypack, err)
		}
	}
}

func testRemoveSubKeyArgs(d testSubKeysData, t *testing.T) {
	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()
	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession() = (%v, %v)", session, err)
	}
	defer session.Close()

	pkey := d.key.k
	for _, key := range d.keys {
		// fmt.Printf("pkey %v len(d.keys) %v key.k %v\n", pkey, len(d.keys), key.k)
		cmd, _ := k2hdkc.NewRemoveSubKey(pkey, key.k)
		if ok, err := cmd.Execute(session); !ok {
			t.Errorf("NewRemoveSubKey(%q, %q).Execute() = (%v, %v)", pkey, key.k, ok, err)
		}
		if ok := cmd.Result().Bool(); !ok {
			t.Errorf("NewRemoveSubKey(%q, %q).Result().Bool() = %v", pkey, key.k, ok)
		}
		// errno
		if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewRemoveSubKeysCommand(%q).Execute() = %v", pkey, ok)
		}
	}
}

// TestRemoveSubKeyKeyEmpty send a empty binary key.
func testRemoveSubKeyTypeStringEmpty(t *testing.T) {

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
	cmd, err := k2hdkc.NewRemoveSubKey(key, nil)
	if err == nil {
		t.Errorf("NewRemoveSubKey(%q, nil) = (%v, %v)", key, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewRemoveSubKey(%q) = (%v, %q), want %q", key, cmd, err, wantError)
		}
	}
}

// TestRemoveSubKeyKeyTypeUnknown send a invalid type key.
func testRemoveSubKeyKeyTypeUnknown(t *testing.T) {

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
	cmd, err := k2hdkc.NewRemoveSubKey(key, nil)
	if err == nil {
		t.Errorf("NewRemoveSubKey(%q, nil) = (%v, %v), want not nil", key, cmd, err)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewRemoveSubKey(%q) = (%v, %v), want %v", key, cmd, err, wantError)
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
