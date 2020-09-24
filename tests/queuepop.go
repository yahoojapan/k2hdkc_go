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

// TestQueuePopType send a key and a value and get result as a bool.
func testQueuePop(t *testing.T) {

	// 1. define test data.
	prefix := "TestQueuePushPrefix"
	val := "TestQueuePushVal"
	//key := "TestQueuePushKey"
	testData := []testQData{
		// 1. default
		{
			p: []byte(prefix),
			v: []byte(val),
			k: nil,
			f: true,
			c: true,
			w: "",
			e: 0,
			s: false,
		},
		/*
			// 2. default + key
			{
				p: []byte(prefix),
				v: []byte(val),
				k: []byte(key),
				f: true,
				c: true,
				w: "",
				e: 0,
				s: false,
			},
			// 3. default + key + fifo(false)
			{
				p: []byte(prefix),
				v: []byte(val),
				k: []byte(key),
				f: false,
				c: true,
				w: "",
				e: 0,
				s: false,
			},
			// 4. default + key + fifo(false) + attr(false)
			{
				p: []byte(prefix),
				v: []byte(val),
				k: []byte(key),
				f: false,
				c: false,
				w: "",
				e: 0,
				s: false,
			},
			// 5. default + key + fifo(false) + attr(false) + password
			{
				p: []byte(prefix),
				v: []byte(val),
				k: []byte(key),
				f: false,
				c: false,
				w: "pass",
				e: 0,
				s: false,
			},
			// 6. default + key + fifo(false) + attr(false) + password + expire
			{
				p: []byte(prefix),
				v: []byte(val),
				k: []byte(key),
				f: false,
				c: false,
				w: "pass",
				e: 3,
				s: false,
			},
			// 7. default + key + fifo(false) + attr(false) + password + expire + use_string
			{
				p: []byte(prefix),
				v: []byte(val),
				k: []byte(key),
				f: false,
				c: false,
				w: "pass",
				e: 3,
				s: true,
			},
		*/
	}

	for _, d := range testData {
		if d.s {
			if ok, err := clearIfExists(string(d.p)); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.p, ok, err)
			}
		} else {
			if ok, err := clearIfExists(d.p); !ok {
				t.Errorf("clearIfExists(%q) = (%v, %v)", d.p, ok, err)
			}
		}
		if d.s {
			if ok, err := callPushQueue(string(d.p), string(d.v), string(d.k), d.w); !ok {
				t.Errorf("callPushQueue(%q) = (%v, %v)", d.p, ok, err)
			}
		} else {
			if ok, err := callPushQueue(d.p, d.v, d.k, d.w); !ok {
				t.Errorf("callPushQueue(%q) = (%v, %v)", d.p, ok, err)
			}
		}
		testQPopDataCommandArgs(d, t)
	}
}

func testQPopDataCommandArgs(d testQData, t *testing.T) {
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	rcmd, err := k2hdkc.NewQueuePop(d.p)
	if err != nil {
		t.Errorf("NewQueuePop(%q) returned %v", d.p, err)
	}
	rcmd.UseFifo(d.f)
	rcmd.SetEncPass(d.w)
	if ok, err := rcmd.Execute(session); !ok {
		t.Errorf("NewQueuePop(%q, %q).Execute() returned %v err %v", d.p, d.v, ok, err)
	}
	if ok := rcmd.Result().Bool(); !ok {
		t.Errorf("NewQueuePop(%q, %q).Resul().Bool() = %v, want true", d.p, d.v, ok)
	}
	if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewQueuePop(%q, %q).Execute() = %v", d.p, d.v, ok)
	}
	if !d.s {
		if val := rcmd.Result().ValBytes(); len(val) == 0 {
			t.Errorf("NewQueuePop(%q).Resul().ValBytes() returned %q is zero", d.p, val)
		}
		if val := rcmd.Result().ValBytes(); !bytes.Equal(val, d.v) {
			t.Errorf("NewQueuePop(%q).Resul().ValBytes() returned %q doen't match with %q", d.p, val, d.v)
		}
		if d.k != nil {
			if key := rcmd.Result().KeyBytes(); len(key) == 0 {
				t.Errorf("NewQueuePop(%q).Resul().KeyBytes() returned %q is zero", d.p, key)
			}
			if key := rcmd.Result().KeyBytes(); !bytes.Equal(key, d.k) {
				t.Errorf("NewQueuePop(%q).Resul().KeyBytes() returned %q doen't match with %q", d.p, key, d.k)
			}
		}
	}
	// TODO if fifo flag exists, check if we can pop the first value after pushing 2 times.
	// TODO if attr flag exists, what should I check ???
	// TODO if pass exists, check if we get empty without password, and we get data with password.
	// TODO if expire flag exists, check if we can pop before expire, and can't pop after expired.
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
