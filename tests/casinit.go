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

const (
	testCasKeyUint8  = "TestCasInitKeyUint8"
	testCasKeyUint16 = "TestCasInitKeyUint16"
	testCasKeyUint32 = "TestCasInitKeyUint32"
	testCasKeyUint64 = "TestCasInitKeyUint64"
)

// TestCasInitType send a key and a value and get result as a bool.
func testCasInit(t *testing.T) {

	// 1. define test data.
	testData := []testCasInitData{
		// 1. default
		{
			k: []byte(testCasKeyUint8),
			v: "uint8",
			w: "",
			e: 0,
		},
		// 2. default + key(uint16)
		{
			k: []byte(testCasKeyUint16),
			v: "uint16",
			w: "",
			e: 0,
		},
		// 4. default + key(uint32)
		{
			k: []byte(testCasKeyUint32),
			v: "uint32",
			w: "",
			e: 0,
		},
		// 5. default + key(uint64)
		{
			k: []byte(testCasKeyUint64),
			v: "uint64",
			w: "",
			e: 0,
		},
	}

	for _, d := range testData {
		testCasInitDataCommandArgs(d, t)
	}
}

func testCasInitDataCommandArgs(d testCasInitData, t *testing.T) {
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	//client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewSession() = %v", err)
	}
	defer session.Close()

	if d.v == "" {
		rcmd, err := k2hdkc.NewCasInit(d.k)
		if err != nil {
			t.Errorf("NewCasInit(%q) = %v", d.k, err)
		}
		rcmd.SetEncPass(d.w)
		rcmd.SetExpire(d.e)
		if ok, err := rcmd.Execute(session); !ok {
			t.Errorf("NewCasInit(%q).Execute() = (%v, %v)", d.k, ok, err)
		}
		if ok := rcmd.Result().Bool(); !ok {
			t.Errorf("NewCasInit(%q).Resul().Bool() = (%v), want true", d.k, ok)
		}
		if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewCasInit(%q, %q).Execute() = %v", d.k, d.v, ok)
		}
	} else {
		var rcmd *k2hdkc.CasInit
		var err error
		switch d.v {
		case "uint8":
			{
				var val uint8 = 1
				rcmd, err = k2hdkc.NewCasInitWithValue(d.k, val)
			}
		case "uint16":
			{
				var val uint16 = 1
				rcmd, err = k2hdkc.NewCasInitWithValue(d.k, val)
			}
		case "uint32":
			{
				var val uint32 = 1
				rcmd, err = k2hdkc.NewCasInitWithValue(d.k, val)
			}
		case "uint64":
			{
				var val uint64 = 1
				rcmd, err = k2hdkc.NewCasInitWithValue(d.k, val)
			}
		default:
			{
				err = errors.New("unknown data format")
			}
		}
		if err != nil {
			t.Errorf("NewCasInitWithValue(%q, %q) returned %v", d.k, d.v, err)
		}
		rcmd.SetEncPass(d.w)
		rcmd.SetExpire(d.e)
		if ok, err := rcmd.Execute(session); !ok {
			t.Errorf("NewCasInitWithValue(%q, %q).Execute() returned %v err %v", d.k, d.v, ok, err)
		}
		if ok := rcmd.Result().Bool(); !ok {
			t.Errorf("NewCasInitWithValue(%q, %q).Resul().Bool() returned ok %v true", d.k, d.v, ok)
		}
		if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewCasInitWithValue(%q, %q).Execute() = %v", d.k, d.v, ok)
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
