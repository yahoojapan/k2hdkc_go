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

// TestGetSubKeysTypeByte send a binary key and get result as a bool.
func testGetSubKeys(t *testing.T) {

	// 1. define test data.
	testData := []testSubKeysData{
		{
			key: kv{
				k: []byte("getsubkeys1_parent"),
				v: []byte("p1"),
				p: "",
				s: true,
			},
			keys: []kv{
				{
					k: []byte("getsubkeys1_sub1"),
					v: []byte("s1"),
					p: "",
					s: true,
				},
				{
					k: []byte("getsubkeys1_sub2"),
					v: []byte("s2"),
					p: "",
					s: true,
				},
			},
		},
	}
	// 2. exec tests
	for _, d := range testData {
		// // 1. add children
		skeys := make([]string, len(d.keys))
		for i, kv := range d.keys {
			if kv.s {
				if ok, err := clearIfExists(string(kv.k)); !ok {
					t.Errorf("clearIfExists(%q) = (%v, %v)", kv.k, ok, err)
				}
				if ok, err := saveData(string(kv.k), string(kv.v), kv.p); !ok {
					t.Errorf("saveData(%q, %q, %q) = (%v, %v)", string(kv.k), string(kv.v), kv.p, ok, err)
				}
				skeys[i] = string(kv.k)
				//fmt.Printf("saveData kv.k %v\n", string(kv.k))
			} else {
				// TODO binary test
			}
		}
		// 2. add a parent.
		if d.key.s {
			if ok, err := clearIfExists(string(d.key.k)); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", string(d.key.k), ok, err)
			}
			if ok, err := saveData(string(d.key.k), string(d.key.v), d.key.p); !ok {
				t.Errorf("saveData(%q, %q, %q) = (%v, %v)", string(d.key.k), string(d.key.v), d.key.p, ok, err)
			}
			// 3. add the child as a subkey of the parent.
			//fmt.Printf("callSetSubkeys(%q, %q)\n", string(d.key.k), skeys)
			if ok, err := callSetSubkeys(string(d.key.k), skeys); !ok {
				t.Errorf("callSetSubkeys(%q,  %q) = (%v, %v)", string(d.key.k), skeys, ok, err)
			}
			if cnt, keypack, err := callGetSubkeysString(string(d.key.k)); cnt != len(skeys) {
				t.Errorf("callGetSubkeys(%q) = (cnt=%v, keypack=%v, err=%v)", string(d.key.k), cnt, keypack, err)
			}
		} else {
			// TODO binary test
		}
		// testGetSubKeysArgs(d, t)
	}
}

func testGetSubKeysArgs(d testSubKeysData, t *testing.T) {

	// execute a setsubkeys command.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()
	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(%v) (%v, %v)", client, session, err)
	}
	defer session.Close()

	// 1. get subkeys
	skeys := d.keys
	if d.key.s {
		pkey := string(d.key.k)
		cmd, _ := k2hdkc.NewGetSubKeys(pkey)
		if ok, err := cmd.Execute(session); !ok {
			t.Errorf("NewGetSubKeys(%q).Execute() = (%v, %v)", pkey, ok, err)
		}
		// string
		data := cmd.Result().String()
		if len(data) != len(skeys) {
			t.Errorf("NewGetSubKeys(%q).Result().String() = %v, len(bin) = %v, len(skeys) = %v", pkey, data, len(data), len(skeys))
		}
		for i, key := range skeys {
			if data[i] != string(key.k) {
				t.Errorf("NewGetSubKeys(%q).Result().String() = false, bin[%v] = %v, key.k = %v", pkey, i, data[i], string(key.k))
			}
		}
		// errno
		if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewGetSubKeys(%q).Execute() = %v", pkey, ok)
		}
	} else {
		pkey := d.key.k
		cmd, _ := k2hdkc.NewGetSubKeys(pkey)
		if ok, err := cmd.Execute(session); !ok {
			t.Errorf("NewGetSubKeys(%q).Execute() = (%v, %v)", pkey, ok, err)
		}
		// bytes
		bin := cmd.Result().Bytes()
		if len(bin) != len(skeys) {
			t.Errorf("NewGetSubKeys(%q).Result().Bytes() = %v, len(bin) = %v, len(skeys) = %v", pkey, bin, len(bin), len(skeys))
		}
		for i, key := range skeys {
			if !bytes.Equal(bin[i], key.k) {
				t.Errorf("NewGetSubKeys(%q).Result().Bytes() = false, bin[%v] = %v, key.k = %v", pkey, i, bin[i], key.k)
			}
		}
		// errno
		if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewGetSubKeys(%q).Execute() = %v", pkey, ok)
		}
	}
}

// TestGetSubKeysKeyEmpty send a empty binary key.
func testGetSubKeysTypeStringEmpty(t *testing.T) {

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
	cmd, err := k2hdkc.NewGetSubKeys(key)
	if err == nil {
		t.Errorf("NewGetSubKeys(%v) = (%v, %v)", key, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewGetSubKeys(%q) = (%v, %q), want %q", key, cmd, err, wantError)
		}
	}
}

// TestGetSubKeysKeyTypeUnknown send a invalid type key.
func testGetSubKeysKeyTypeUnknown(t *testing.T) {

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
	cmd, err := k2hdkc.NewGetSubKeys(key)
	if err == nil {
		t.Errorf("NewGetSubKeys(%q) = (%v, %v), want not nil", key, cmd, err)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewGetSubKeys(%q) = (%v, %v), want %v", key, cmd, err, wantError)
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
