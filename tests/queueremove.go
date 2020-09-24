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

// TestQueueRemoveType send a key and a value and get result as a bool.
func testQueueRemove(t *testing.T) {
	// 1. define test data.
	prefix := "TestQueuePushPrefix"
	testData := []testQRemoveData{
		// 1. default
		{
			p: []byte(prefix),
			c: 1,
			f: true,
			w: "",
			u: true,
		},
	}

	for _, d := range testData {
		if ok, err := clearIfExists(d.p); !ok {
			t.Errorf("clearIfExists(%q) = (%v, %v)", d.p, ok, err)
		}
		if ok, err := callPushQueue(d.p, []byte("sample_value"), []byte("sample_key"), d.w); !ok {
			t.Errorf("callPushQueue(%q) = (%v, %v)", d.p, ok, err)
		}
		testQRemoveDataCommandArgs(d, t)
	}
}

func testQRemoveDataCommandArgs(d testQRemoveData, t *testing.T) {
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()
	rcmd, err := k2hdkc.NewQueueRemove(d.p, d.c)
	if err != nil {
		t.Errorf("NewQueueRemove(%q) returned %v", d.p, err)
	}
	rcmd.UseFifo(d.f)
	rcmd.SetEncPass(d.w)
	if ok, err := rcmd.Execute(session); !ok {
		t.Errorf("NewQueueRemove(%q, %v).Execute() returned %v err %v", d.p, d.c, ok, err)
	}
	if ok := rcmd.Result().Bool(); !ok {
		t.Errorf("NewQueueRemove(%q, %q).Resul().Bool() = %v, want true", d.p, d.c, ok)
	}
	if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewQueueRemove(%q, %q).Execute() = %v", d.p, d.c, ok)
	}
	// TODO if fifo flag exists, check if we can pop the first value after pushing 2 times.
	// TODO if count flag exists, check if we can remove the number of count value.
	// TODO if pass exists, check if we get empty without password, and we get data with password.
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
