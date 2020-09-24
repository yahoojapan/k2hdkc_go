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

// TestSetTypeByte send a binary key and a binary value and get result as a bool.
func testSetSubKeys(t *testing.T) {

	// 1. define test data.
	testData := []testSubKeysData{
		{
			d: "binary",
			key: kv{
				k: []byte("setsubkeys1_parent"),
				v: []byte("p1"),
				p: "",
			},
			keys: []kv{
				{
					k: []byte("setsubkeys1_sub1"),
					v: []byte("s1"),
					p: "",
				},
				{
					k: []byte("setsubkeys1_sub2"),
					v: []byte("s2"),
					p: "",
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
				// fmt.Printf("kv.k %v\n", string(kv.k))
				if ok, err := saveData(string(kv.k), string(kv.v), kv.p); !ok {
					t.Errorf("saveData(%q, %q, %q) = (%v, %v)", string(kv.k), string(kv.v), kv.p, ok, err)
				}
				skeys[i] = string(kv.k)
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
			if ok, err := callSetSubkeys(string(d.key.k), skeys); !ok {
				t.Errorf("callSetSubkeys(%q,  %q) = (%v, %v)", string(d.key.k), skeys, ok, err)
			}
		} else {
			// TODO binary test
		}
		testSetSubKeysArgs(d, t)
		if d.key.s {
			cnt, keypack, err := callGetSubkeys(string(d.key.k))
			if cnt != 2 {
				t.Errorf("callGetSubkeys(%q) = (cnt=%v, %v, %v), want cnt=2", d.key.k, cnt, keypack, err)
			}
			if d.key.s {
				for i, key := range d.keys {
					if string(keypack[i]) != string(key.k) {
						t.Errorf("callGetSubkeys(%q) keypack[%v] = %v, key.k = %v", key.k, i, keypack[i], string(key.k))
					}
				}
			} else {
				for i, key := range d.keys {
					if !bytes.Equal(key.k, keypack[i]) {
						t.Errorf("callGetSubkeys(%q), key.k=%v keypack[%v]=%v", d.key.k, key.k, i, keypack[i])
					}
				}
			}
		}
	}
}

func testSetSubKeysArgs(d testSubKeysData, t *testing.T) {
	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession(client) = (%v, %v)", session, err)
	}
	defer session.Close()

	pkey := d.key.k
	skeys := make([][]byte, len(d.keys))
	for i, key := range d.keys {
		skeys[i] = key.k
	}
	// fmt.Printf("pkey %v len(d.keys) %v len(skeys) %v\n", pkey, len(d.keys), len(skeys))

	cmd, _ := k2hdkc.NewSetSubKeys(pkey, skeys)
	if ok, err := cmd.Execute(session); !ok {
		t.Errorf("NewSetSubKeys(%q, %q).Execute() = (%v, %v)", pkey, skeys, ok, err)
	}
	if ok := cmd.Result().Bool(); !ok {
		t.Errorf("NewSetSubKeys(%q, %q).Execute() = %v", pkey, skeys, ok)
	}
	// errno
	if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewSetSubKeys(%q).Execute() = %v", pkey, ok)
	}
}

// TestSetSubKeysTypeEmpty simulates sending an empty key data.
func testSetSubKeysTypeEmpty(t *testing.T) {
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
	cmd, err := k2hdkc.NewSetSubKeys(key, want)
	if err == nil {
		t.Errorf("NewSetSubKeys(%q, %q) = (%v, %v), want not nil", key, want, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewSetSubKeys(%q, %q) = (%v, %v), want %q", key, want, cmd, err, wantError)
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
