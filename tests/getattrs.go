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

type testGetAttrsData struct {
	key kv // key
}

// TestGetAttrsTypeByte send a binary key and get result as a bool.
func testGetAttrsTypeByte(t *testing.T) {

	// 1. define test data.
	testData := []testGetAttrsData{
		{
			key: kv{
				k: []byte("TestGetAttrsTypeByte"),
				v: []byte("wantu"),
				p: "",
			},
		},
	}
	for _, d := range testData {
		if ok, err := clearIfExists(d.key.k); !ok {
			t.Errorf("clearIfExists(%q) returned %v err %v", d.key.k, ok, err)
		}
		// 2.1 a key
		if ok, err := saveData(d.key.k, d.key.v, d.key.p); !ok {
			t.Errorf("saveData(%q, %q, %q) returned %v err %v", d.key.k, d.key.v, d.key.p, ok, err)
		}
		testGetAttrsArgs(d, t)
	}
}

func testGetAttrsArgs(d testGetAttrsData, t *testing.T) {
	// 3. connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	// 4. execute a getattrs command.
	pkey := d.key.k
	cmd, _ := k2hdkc.NewGetAttrs(pkey)
	if ok, err := cmd.Execute(session); !ok {
		t.Errorf("NewGetAttrs(%q).Execute() returned %v err %v", pkey, ok, err)
	}
	// get attrs
	bin := cmd.Result().Bytes()
	if len(bin) == 0 {
		t.Errorf("cmd.Result().Byte() returned %v len(%v) != 0", bin, len(bin))
	}
	result := cmd.Result().String()
	if len(result) == 0 {
		t.Errorf("cmd.Result().String() returned %v len(%v) != 0", result, len(result))
	}
	if _, ok := result["mtime"]; !ok {
		t.Errorf("cmd.Result().String() contains no mtime(%v), want mtime", result)
	}
	if ok := cmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("cmd.Execute(%q).Execute() = %v", pkey, ok)
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
