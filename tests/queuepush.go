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

// TestQueuePushType send a key and a value and get result as a bool.
func testQueuePush(t *testing.T) {

	// 1. define test data.
	testData := []testQData{
		// 1. default
		{
			p: []byte("push_prefix_2"),
			v: []byte("push_prefix_2_val"),
			k: nil,
			f: true,
			c: true,
			w: "",
			e: 0,
			s: false,
		},
		// 2. default + key
		{
			p: []byte("push_prefix_2"),
			v: []byte("push_prefix_2_val"),
			k: []byte("push_prefix_2_key"),
			f: true,
			c: true,
			w: "",
			e: 0,
			s: false,
		},
		/*
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
		testQPushDataCommandArgs(d, t)

		// check results.
		/*
			var rval, rkey []byte
			var err error
			if rval, rkey, err = callPopQueue(d.p, d.v, d.k, d.w); err != nil {
				t.Errorf("callQueuePop(%q, %q, %q, %q) = (%v, %v, %v)", d.p, d.v, d.k, d.w, rval, rkey, err)
			}
			if !bytes.Equal(rval, d.v) {
				t.Errorf("callQueuePop(%q, %q, %q, %q) = (%v, %v, %v), want rval == d.v", d.p, d.v, d.k, d.w, rval, rkey, err)
			}
			if !bytes.Equal(rkey, d.k) {
				t.Errorf("callQueuePop(%q, %q, %q, %q) = (%v, %v, %v), want rval == d.v", d.p, d.v, d.k, d.w, rval, rkey, err)
			}
		*/
	}
}

func testQPushDataCommandArgs(d testQData, t *testing.T) {
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	var rcmd *k2hdkc.QueuePush
	if len(d.k) != 0 {
		rcmd, err = k2hdkc.NewQueuePushWithKey(d.p, d.v, d.k)
	} else {
		rcmd, err = k2hdkc.NewQueuePush(d.p, d.v)
	}
	if err != nil {
		t.Errorf("NewQueuePush(%q, %q) = %v", d.p, d.v, err)
	}
	rcmd.UseFifo(d.f)
	rcmd.SetAttr(d.c)
	rcmd.SetEncPass(d.w)
	rcmd.SetExpire(d.e)
	// fmt.Printf("rcmd %v\n", rcmd)
	if ok, err := rcmd.Execute(session); !ok {
		t.Errorf("NewQueuePush(%q, %q).Execute() = (%v, %v)", d.p, d.v, ok, err)
	}
	if ok := rcmd.Result().Bool(); !ok {
		t.Errorf("NewQueuePush(%q, %q).Resul().Bool() = %v, want true", d.p, d.v, ok)
	}
	if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewQueuePush(%q, %q).Execute() = %v", d.p, d.v, ok)
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
