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

func testSessionNew(t *testing.T) {
	// connect with a cluster.
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	defer client.Close()
	client.SetLogSeverity(k2hdkc.SeverityInfo)

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()
}
func testSessionNewError(t *testing.T) {
	// connect with a cluster.
	session, err := k2hdkc.NewSession(nil)
	wantError := "client is nil"
	if err.Error() != wantError {
		t.Errorf("NewSession(nil) returned %v want %v", err, wantError)
	}
	if session != nil {
		defer session.Close()
	}
}

func testSessionCreate(t *testing.T) {
	session, err := k2hdkc.NewClient("../cluster/slave.yaml", 8031).CreateSession()
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
