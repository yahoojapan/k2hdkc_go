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

// SetAll holds a key and subkeys. command execs C.k2hdkc_pm_set_subkeys and stores the result.
// bool k2hdkc_pm_set_all_wa(
//    k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength,
//    const unsigned char* pval, size_t vallength,
//    const PK2HDKCKEYPCK pskeypck, int skeypckcnt, const char* encpass, const time_t* expire)
// TestSetTypeByte send a binary key and a binary value and get result as a bool.
func testSetAll(t *testing.T) {

	// 1. define test data.
	testData := []testSubKeysData{
		{
			d: "binary",
			key: kv{
				k: []byte("setall1_parent"),
				v: []byte("p1"),
				p: "",
				e: 0,
			},
			keys: []kv{
				{
					k: []byte("setall_sub1"),
					v: []byte("s1"),
					p: "",
				},
				{
					k: []byte("setall_sub2"),
					v: []byte("s2"),
					p: "",
				},
			},
		},
	}
	// 2. exec tests
	for _, d := range testData {
		if ok, err := clearIfExists(d.key.k); !ok {
			t.Errorf("clearIfExists(%q) returned %v err %v", d.key.k, ok, err)
		}
		// 2.1 a key
		if ok, err := saveData(d.key.k, d.key.v, d.key.p); !ok {
			t.Errorf("saveData(%q, %q, %q) returned %v err %v", d.key.k, d.key.v, d.key.p, ok, err)
		}
		// 2.2 keys of a subkey
		for _, key := range d.keys {
			if ok, err := saveData(key.k, key.v, key.p); !ok {
				t.Errorf("saveData(%q, %q, %q) returned %v err %v", key.k, key.v, key.p, ok, err)
			}
		}
		testSetAllArgs(d, t)
		cnt, keypack, err := callGetSubkeys(d.key.k)
		if cnt != 1 {
			t.Errorf("callGetSubkeys(%q) = (cnt:%v keypack:%v err:%v) want cnt=1", d.key.k, cnt, keypack, err)
		}
		if cnt == 1 && !bytes.Equal(d.keys[0].k, keypack[0]) {
			t.Errorf("callGetSubkeys(%q) d.keys[0].k:%v keypack[0]:%v want d.keys[0].k == keypack[0]", d.key.k, d.keys[0].k, keypack[0])
		}
	}
}

func testSetAllArgs(d testSubKeysData, t *testing.T) {
	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession() = (%v, %v)", session, err)
	}
	defer session.Close()

	pkey := d.key.k
	skeys := make([][]byte, 1) // stores only one subkey.
	skeys[0] = d.keys[0].k
	cmd, _ := k2hdkc.NewSetAll(pkey, d.key.v, skeys)
	if ok, err := cmd.Execute(session); !ok {
		t.Errorf("NewSetAll(%q, %q, %q).Execute() = (%v, %v)", pkey, d.key.v, skeys, ok, err)
	}
	if ok := cmd.Result().Bool(); !ok {
		t.Errorf("NewSetAll(%q, %q, %q).Result().Bool() = %v", pkey, d.key.v, skeys, ok)
	}
	// errno
	if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewSetAll(%q).Execute() = %v", pkey, ok)
	}
}

// TestSetAllKeyEmpty send a empty binary key.
func testSetAllTypeStringEmpty(t *testing.T) {

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
	cmd, err := k2hdkc.NewSetAll(key, nil, nil)
	if err == nil {
		t.Errorf("NewSetAll(%q, nil, nil) = (%v, %v)", key, cmd, err)
	} else {
		wantError := "len(key) is zero"
		if err.Error() != wantError {
			t.Errorf("NewSetAll(%q, nil, nil) = (%v, %q), want %q", key, cmd, err, wantError)
		}
	}
}

// TestSetAllKeyTypeUnknown send a invalid type key.
func testSetAllKeyTypeUnknown(t *testing.T) {

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
	cmd, err := k2hdkc.NewSetAll(key, nil, nil)
	if err == nil {
		t.Errorf("NewSetAll(%q, nil, nil) = (%v, nil), want not nil", key, cmd)
	} else {
		wantError := "unsupported key data format uint8"
		if err.Error() != wantError {
			t.Errorf("NewSetAll(%q, nil, nil) = (%v, %v), want %v", key, cmd, err, wantError)
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
