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
	"errors"
	"testing"

	"github.com/yahoojapan/k2hdkc_go/k2hdkc"
)

// TestCasGetType send a key and a value and get result as a bool.
func testCasGet(t *testing.T) {
	// TODO this test fails now and I investigate the reason.
	t.Skip("skipping a casget test ")

	// 1. define test data.
	testData := []testCasGetData{
		{
			k:  []byte(testCasKeyUint8),
			o:  []byte{0},
			n:  []byte{0},
			w:  "",
			vl: 8,
		},
		{
			k:  []byte(testCasKeyUint16),
			o:  []byte{0, 0},
			n:  []byte{0, 0},
			w:  "",
			vl: 16,
		},
		{
			k:  []byte(testCasKeyUint32),
			o:  []byte{0, 0, 0, 0},
			n:  []byte{0, 0, 0, 0},
			w:  "",
			vl: 32,
		},
		{
			k:  []byte(testCasKeyUint64),
			o:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			n:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			w:  "",
			vl: 64,
		},
	}

	for _, d := range testData {
		if ok, err := clearIfExists(d.k); !ok {
			t.Errorf("clearIfExists(%q) = (%v, %v)", d.k, ok, err)
		}
		// callCasSet(key []byte, old []byte, new []byte, pass string, expire uint64) (bool, error) {
		if ok, err := callCasSet(d.k, d.o, d.n, d.w); !ok {
			t.Errorf("callCasSet(%q) = (%v, %v)", d.k, ok, err)
		}
		testCasGetDataCommandArgs(d, t)
	}
}

func testCasGetDataCommandArgs(d testCasGetData, t *testing.T) {
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	var rcmd *k2hdkc.CasGet
	switch d.vl {
	case 8:
		{
			rcmd, err = k2hdkc.NewCasGetWithCasType(d.k, 8)
		}
	case 16:
		{
			rcmd, err = k2hdkc.NewCasGetWithCasType(d.k, 16)
		}
	case 32:
		{
			rcmd, err = k2hdkc.NewCasGetWithCasType(d.k, 32)
		}
	case 64:
		{
			rcmd, err = k2hdkc.NewCasGetWithCasType(d.k, 64)
		}
	default:
		{
			err = errors.New("unknown data format")
		}
	}
	if err != nil {
		t.Errorf("NewCasGet(%q, %q) returned %v", d.k, d.vl, err)
	}
	rcmd.SetEncPass(d.w)

	if ok, err := rcmd.Execute(session); !ok {
		t.Errorf("NewCasGet(%q, %v).Execute() returned %v err %v", d.k, d.vl, ok, err)
	}
	if val := rcmd.Result().Bytes(); val == nil {
		t.Errorf("NewCasGet(%q, %v).Resul().Bytes() returned val %q", d.k, d.vl, val)
	}
	if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewCasGet(%q, %v).Execute() = %v", d.k, d.vl, ok)
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
