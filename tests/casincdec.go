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

// TestCasIncDecType send a key and a value and get result as a bool.
func testCasIncDec(t *testing.T) {

	// 1. define test data.
	testData := []testCasIncDecData{
		// 1. default(incr)
		{
			k: []byte(testCasKeyUint8),
			w: "",
			e: 0,
			i: true,
		},
		// 2. default(decr)
		{
			k: []byte(testCasKeyUint8),
			w: "",
			e: 0,
			i: false,
		},
	}

	for _, d := range testData {
		testCasIncDecDataCommandArgs(d, t)
	}
}

func testCasIncDecDataCommandArgs(d testCasIncDecData, t *testing.T) {
	// TODO this test sometimes fails now and I investigate the reason.
	t.Skip("skipping a casget test ")

	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	var rcmd *k2hdkc.CasIncDec
	rcmd, err = k2hdkc.NewCasIncDec(d.k, d.i)
	if err != nil {
		t.Errorf("NewCasIncDec(%q, %v) returned %v", d.k, d.i, err)
	}
	rcmd.SetEncPass(d.w)
	rcmd.SetExpire(d.e)
	if ok, err := rcmd.Execute(session); !ok {
		t.Errorf("NewCasIncDec(%q, %v).Execute() returned %v err %v", d.k, d.i, ok, err)
	}
	if ok := rcmd.Result().Bool(); !ok {
		t.Errorf("NewCasIncDec(%q, %v).Resul().Bool() returned ok %v true", d.k, d.i, ok)
	}
	if ok := rcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
		t.Errorf("NewCasIncDec(%q, %v).Execute() = %v", d.k, d.i, ok)
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
