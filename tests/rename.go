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
	"time"

	"github.com/yahoojapan/k2hdkc_go/k2hdkc"
)

// TestRenameType send a key and a value and get result as a bool.
func testRename(t *testing.T) {

	// 1. define test data.
	oldKey := "rename_old1"
	newKey := "rename_new1"
	parentKey := "rename_parent1"
	testData := []testRenameData{
		// 1. default
		{
			o: []byte(oldKey),
			n: []byte(newKey),
			p: []byte(""),
			c: true,
			w: "",
			e: 0,
			s: false,
		},
		// 2. default + parent
		{
			o: []byte(oldKey),
			n: []byte(newKey),
			p: []byte(parentKey),
			c: true,
			w: "",
			e: 0,
			s: false,
		},
		// 3. default + parent + attr(false)
		{
			o: []byte(oldKey),
			n: []byte(newKey),
			p: []byte(parentKey),
			c: false,
			w: "",
			e: 0,
			s: false,
		},
		// 4. default + parent + attr(false) + password
		{
			o: []byte(oldKey),
			n: []byte(newKey),
			p: []byte(parentKey),
			c: false,
			w: "pass",
			e: 0,
			s: false,
		},
		// TODO this test fails now and I investigate the reason.
		/*
			// 5. default + parent + attr(false) + password + expire
			{
				o: []byte(oldKey),
				n: []byte(newKey),
				p: []byte(parentKey),
				c: false,
				w: "pass",
				e: 3,
				s: false,
			},

				// 6. default + parent + attr(false) + password + expire + string
				{
					o: []byte(oldKey),
					n: []byte(newKey),
					p: []byte(parentKey),
					c: false,
					w: "pass",
					e: 3,
					s: false,
				},
		*/
	}

	for _, d := range testData {
		// 1. initialize
		if !d.s {
			if ok, err := clearIfExists(d.o); !ok {
				t.Errorf("clearIfExists(%q) returned %v err %v", d.o, ok, err)
			}
			if ok, err := clearIfExists(d.n); !ok {
				t.Errorf("clearIfExists(%q) returned %v err %v", d.n, ok, err)
			}
			if len(d.p) > 0 {
				if ok, err := clearIfExists(d.p); !ok {
					t.Errorf("clearIfExists(%q) returned %v err %v", d.p, ok, err)
				}
			}

			// 2. make old and parent
			if ok, err := saveData(d.o, d.o, d.w); !ok {
				t.Errorf("saveData(%q, %q, %q) returned %v err %v", d.o, d.o, d.w, ok, err)
			}
			// 3. make subkey.
			if len(d.p) != 0 {
				if ok, err := saveData(d.p, d.p, d.w); !ok {
					t.Errorf("saveData(%q, %q, %q) returned %v err %v", d.p, d.p, d.w, ok, err)
				}
				skeys := make([][]byte, 1)
				skeys[0] = d.o
				if ok, err := callSetSubkeys(d.p, skeys); !ok {
					t.Errorf("callSetSubkeys(%q, %q) returned %v err %v", d.p, skeys, ok, err)
				}
			}
		} else {
			if ok, err := clearIfExists(string(d.o)); !ok {
				t.Errorf("clearIfExists(%q) returned %v err %v", string(d.o), ok, err)
			}
			if ok, err := clearIfExists(string(d.n)); !ok {
				t.Errorf("clearIfExists(%q) returned %v err %v", string(d.n), ok, err)
			}
			if len(d.p) > 0 {
				if ok, err := clearIfExists(string(d.p)); !ok {
					t.Errorf("clearIfExists(%q) returned %v err %v", string(d.p), ok, err)
				}
			}
			// 2. make old and parent
			if ok, err := saveData(string(d.o), string(d.o), d.w); !ok {
				t.Errorf("saveData(%q, %q, %q) returned %v err %v", string(d.o), string(d.o), d.w, ok, err)
			}
			// 3. make subkey.
			if len(d.p) != 0 {
				if ok, err := saveData(string(d.p), string(d.p), d.w); !ok {
					t.Errorf("saveData(%q, %q, %q) returned %v err %v", string(d.p), string(d.p), d.w, ok, err)
				}
				skeys := make([]string, 1)
				skeys[0] = string(d.o)
				if ok, err := callSetSubkeys(string(d.p), skeys); !ok {
					t.Errorf("callSetSubkeys(%q, %q) returned %v err %v", string(d.p), skeys, ok, err)
				}
			}
		}
		testRenameDataCommandArgs(d, t)
	}
}

func testRenameDataCommandArgs(d testRenameData, t *testing.T) {
	// rename d.o d.n
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	rcmd, err := k2hdkc.NewRename(d.o, d.n)
	if err != nil {
		t.Errorf("NewRename(%q, %q) returned %v", d.o, d.n, err)
	}
	if len(d.p) != 0 {
		rcmd.SetParentKey(d.p)
	}
	rcmd.SetAttr(d.c)
	rcmd.SetEncPass(d.w)
	rcmd.SetExpire(d.e)
	if ok, err := rcmd.Execute(session); !ok {
		t.Errorf("NewRename(%q, %q).Execute() returned %v err %v", d.o, d.n, ok, err)
	}
	if ok := rcmd.Result().Bool(); !ok {
		t.Errorf("NewRename(%q, %q).Resul().Bool() returned ok %v true", d.o, d.n, ok)
	}

	// if parentKey is not nil, check if subkey is renamed old to new.
	if len(d.p) != 0 {
		cmd, _ := k2hdkc.NewGetSubKeys(d.p)
		if ok, err := cmd.Execute(session); !ok {
			t.Errorf("NewGetSubKeys(%q).Execute() returned %v err %v", d.p, ok, err)
		}
		// check if parenet has the new subkey by calling k2hdkc_pm_get_subkeys.
		result := cmd.Result().Bytes()
		for i, k := range result {
			if !bytes.Equal(k, d.n) {
				t.Errorf("NewGetSubKeys(%q) should match result[%v] %v with key.k %v", d.p, i, string(result[i]), string(d.n))
			}
		}
	}

	// I should not get the old soon after renameing.
	if !d.s {
		gcmd, gerr := k2hdkc.NewGet(d.o)
		if gerr != nil {
			t.Errorf("NewGet(%q) returned %v", d.o, gerr)
		}
		if d.w != "" {
			gcmd.SetEncPass(d.w)
		}
		if ok, gerr := gcmd.Execute(session); !ok {
			t.Errorf("NewGet(%q).Execute() returned %v err %v", d.o, ok, gerr)
		}
		if bin := gcmd.Result().Bytes(); len(bin) != 0 {
			t.Errorf("ecmd.Result().Byte() returned %v len(%v) != 0", bin, len(bin))
		}
		if ok := gcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NODATA" {
			t.Errorf("NewGet(%q).Execute() = %v", d.o, ok)
		}

		// I should get the new key before data expires.
		gcmd, gerr = k2hdkc.NewGet(d.n)
		if gerr != nil {
			t.Errorf("NewGet(%q) returned %v", d.n, gerr)
		}
		if d.w != "" {
			gcmd.SetEncPass(d.w)
		}
		if ok, gerr := gcmd.Execute(session); !ok {
			t.Errorf("NewGet(%q).Execute() returned %v err %v", d.n, ok, gerr)
		}
		if bin := gcmd.Result().Bytes(); len(bin) == 0 || !bytes.Equal(bin, d.o) {
			t.Errorf("ecmd.Result().Byte() returned false. want %v len(%v) != 0 %v", bin, d.o, len(bin))
		}
		if ok := gcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewGet(%q).Execute() = %v", d.n, ok)
		}
	} else {
		gcmd, gerr := k2hdkc.NewGet(string(d.o))
		if gerr != nil {
			t.Errorf("NewGet(%q) returned %v", string(d.o), gerr)
		}
		if d.w != "" {
			gcmd.SetEncPass(d.w)
		}
		if ok, gerr := gcmd.Execute(session); !ok {
			t.Errorf("NewGet(%q).Execute() returned %v err %v", string(d.o), ok, gerr)
		}
		if text := gcmd.Result().String(); len(text) != 0 {
			t.Errorf("getcmd.Result().String() returned %v len(%v) != 0", text, len(text))
		}
		if ok := gcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NODATA" {
			t.Errorf("NewGet(%q).Execute() = %v", string(d.o), ok)
		}

		// I should get the new key before data expires.
		gcmd, gerr = k2hdkc.NewGet(string(d.n))
		if gerr != nil {
			t.Errorf("NewGet(%q) returned %v", string(d.n), gerr)
		}
		if d.w != "" {
			gcmd.SetEncPass(d.w)
		}
		if ok, gerr := gcmd.Execute(session); !ok {
			t.Errorf("NewGet(%q).Execute() returned %v err %v", string(d.n), ok, gerr)
		}
		if text := gcmd.Result().String(); len(text) == 0 || text != string(d.o) {
			t.Errorf("getcmd.Result().String() returned %v want %v len(%v) != 0", text, string(d.o), len(text))
		}
		if ok := gcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
			t.Errorf("NewGet(%q).Execute() = %v", string(d.n), ok)
		}
	}
	// 6. I should not get the new key after data expires.
	if d.e != 0 {
		time.Sleep(time.Duration(d.e+2) * time.Second)
		if !d.s {
			gcmd, gerr := k2hdkc.NewGet(d.n)
			if gerr != nil {
				t.Errorf("NewGet(%q) returned %v", d.n, gerr)
			}
			if d.w != "" {
				gcmd.SetEncPass(d.w)
			}
			if ok, gerr := gcmd.Execute(session); !ok {
				t.Errorf("NewGet(%q).Execute() returned %v err %v", d.n, ok, gerr)
			}
			if bin := gcmd.Result().Bytes(); len(bin) != 0 {
				t.Errorf("ecmd.Result().Byte() returned %v len(%v) != 0", bin, len(bin))
			}
			if text := gcmd.Result().String(); len(text) != 0 {
				t.Errorf("getcmd.Result().String() returned %v len(%v) != 0", text, len(text))
			}
			if ok := gcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
				t.Errorf("NewGet(%q).Execute() = %v", d.n, ok)
			}
		} else {
			gcmd, gerr := k2hdkc.NewGet(string(d.n))
			if gerr != nil {
				t.Errorf("NewGet(%q) returned %v", string(d.n), gerr)
			}
			if d.w != "" {
				gcmd.SetEncPass(d.w)
			}
			if ok, gerr := gcmd.Execute(session); !ok {
				t.Errorf("NewGet(%q).Execute() returned %v err %v", string(d.n), ok, gerr)
			}
			if text := gcmd.Result().String(); len(text) != 0 {
				t.Errorf("getcmd.Result().String() returned %v len(%v) != 0", text, len(text))
			}
			if ok := gcmd.Result().Error(); ok != "DKC_RES_SUCCESS DKC_RES_SUBCODE_NOTHING" {
				t.Errorf("NewGet(%q).Execute() = %v", string(d.n), ok)
			}
		}
	}

}

func testRenameParent(t *testing.T) {
	// rename d.o d.n
	client := k2hdkc.NewClient("../cluster/slave.yaml", 8031)
	client.SetLogSeverity(k2hdkc.SeverityInfo)
	defer client.Close()

	session, err := k2hdkc.NewSession(client)
	if err != nil {
		t.Errorf("NewClient(../cluster/slave.yaml, 8031).CreateSession() returned %v", err)
	}
	defer session.Close()

	oldKey := "TestRenameParentArgsOld"
	newKey := "TestRenameParentArgsNew"
	rcmd, err := k2hdkc.NewRename(oldKey, newKey)
	if err != nil {
		t.Errorf("NewRename(%q, %q) returned %v", oldKey, newKey, err)
	}
	var invalidParentKey uint8 = 10
	var ok2 bool
	var err2 error
	ok2, err2 = rcmd.SetParentKey(invalidParentKey)
	if err2 == nil {
		t.Errorf("rcmd.SetParentKey(%q) returned (%v, nil), want (false, not_nil)", invalidParentKey, ok2)
	}
	if err2.Error() != "unsupported val data format uint8, want string or []byte" {
		t.Errorf("rcmd.SetParentKey(%q) returned a wrong error message (%q)", invalidParentKey, err2)
	}

	emptyStringParentKey := ""
	var ok3 bool
	var err3 error
	ok3, err3 = rcmd.SetParentKey(emptyStringParentKey)
	if err3 == nil {
		t.Errorf("rcmd.SetParentKey(%q) returned (%v, nil), want (false, not_nil)", emptyStringParentKey, ok3)
	}
	if err3.Error() != "len(parentKey) is zero" {
		t.Errorf("rcmd.SetParentKey(%q) returned a wrong error message (%q)", emptyStringParentKey, err3)
	}

	var emptyBinaryParentKey []byte
	var ok4 bool
	var err4 error
	ok3, err4 = rcmd.SetParentKey(emptyBinaryParentKey)
	if err4 == nil {
		t.Errorf("rcmd.SetParentKey(%q) returned (%v, nil), want (false, not_nil)", emptyBinaryParentKey, ok4)
	}
	if err4.Error() != "len(parentKey) is zero" {
		t.Errorf("rcmd.SetParentKey(%q) returned a wrong error message (%q)", emptyBinaryParentKey, err4)
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
