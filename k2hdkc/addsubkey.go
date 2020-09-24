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

package k2hdkc

import (
	// #cgo CFLAGS: -g -O2 -Wall -Wextra -Wno-unused-variable -Wno-unused-parameter -I. -I/usr/include/k2hdkc
	// #cgo LDFLAGS: -L/usr/lib -lk2hdkc
	// #include <stdlib.h>
	// #include "k2hdkc.h"
	// #include "k2hmacro.h"
	"C"
)
import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"
)

// AddSubKey holds C.k2hdkc_pm_set_subkey_wa arguments and a *AddSubKeyResult.
type AddSubKey struct {
	key    []byte
	skey   []byte
	sval   []byte
	attr   bool
	pass   string
	expire int64
	result *AddSubKeyResult
}

// String returns a text representation of the object.
func (r *AddSubKey) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v, %v]", r.key, r.skey, r.sval, r.attr, r.pass, r.expire, r.result)
}

// AddSubKeyResult holds the result of AddSubKey.Execute().
type AddSubKeyResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *AddSubKeyResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewAddSubKey returns a new AddSubKey.
func NewAddSubKey(k interface{}, sk interface{}, sv interface{}) (*AddSubKey, error) {
	var key []byte
	var skey []byte
	var sval []byte

	// key
	switch k.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", k)
	case string:
		if len(k.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(k.(string))
			buf.WriteRune('\u0000')
			key = buf.Bytes()
		}
	case []byte:
		key = k.([]byte)
	}
	if key == nil || len(key) == 0 {
		return nil, errors.New("len(key) is zero")
	}

	// subkey
	switch sk.(type) {
	default:
		return nil, fmt.Errorf("unsupported subkey data format %T", sk)
	case []byte:
		skey = sk.([]byte)
	case string:
		if len(sk.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(sk.(string))
			buf.WriteRune('\u0000')
			skey = buf.Bytes()
		}
	}
	if skey == nil || len(skey) == 0 {
		return nil, errors.New("len(skey) is zero")
	}

	// subkey value
	switch sv.(type) {
	default:
		return nil, fmt.Errorf("unsupported subkey value data format %T", sv)
	case []byte:
		sval = sv.([]byte)
	case string:
		if len(sv.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(sv.(string))
			buf.WriteRune('\u0000')
			sval = buf.Bytes()
		}
	}
	if sval == nil || len(sval) == 0 {
		return nil, errors.New("len(sval) is zero")
	}

	r := &AddSubKeyResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &AddSubKey{
		key:    key,
		skey:   skey,
		sval:   sval,
		attr:   defaultCheckAttr,
		pass:   "",
		expire: 0,
		result: r,
	}
	return c, nil
}

// Execute calls the C.k2hdkc_pm_set_subkey_wa function that adds subkey to the k2hdkc cluster.
func (r *AddSubKey) Execute(s *Session) (bool, error) {
	// zero r.sval is ok though z.sval == nil is not ok.
	if r.key == nil || len(r.key) == 0 || r.skey == nil || len(r.skey) == 0 || r.sval == nil || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.skey %v, r.sval %v, r.result %v", r.key, r.skey, r.sval, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(unsafe.Pointer(cKey))
	cSkey := C.CBytes(r.skey)
	defer C.free(unsafe.Pointer(cSkey))
	cSval := C.CBytes(r.sval)
	defer C.free(unsafe.Pointer(cSval))
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	r.result.ok = C.k2hdkc_pm_set_subkey_wa(
		s.handler,
		(*C.uchar)(cKey),
		(C.size_t)(len(r.key)),
		(*C.uchar)(cSkey),
		(C.size_t)(len(r.skey)),
		(*C.uchar)(cSval),
		(C.size_t)(len(r.sval)),
		(C._Bool)(r.attr),
		cPass,
		expire)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, fmt.Errorf("C.k2hdkc_pm_set_subkey_wa %v", r.result.ok)
	}
	return true, nil
}

// Result returns the pointer of AddSubKeyResult that has the result of Execute method.
// nil returns if no result exists.
func (r *AddSubKey) Result() *AddSubKeyResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns the result whether C.k2hdkc_pm_set_subkey_wa is success.
func (r *AddSubKeyResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_set_subkey_wa in string format.
func (r *AddSubKeyResult) Error() string {
	return fmt.Sprintf("%v %v",
		C.GoString(C.str_dkcres_result_type(r.resCode)),
		C.GoString(C.str_dkcres_subcode_type(r.subResCode)))
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
