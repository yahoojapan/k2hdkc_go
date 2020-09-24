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

// SetAll holds arguments for C.k2hdkc_pm_set_all_wa and a pointer of SetAllResult.
type SetAll struct {
	key    []byte
	val    []byte
	skeys  [][]byte
	pass   string
	expire int64
	result *SetAllResult
}

// String returns a text representation of the object.
func (r *SetAll) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v]", r.key, r.val, r.skeys, r.pass, r.expire, r.result)
}

// SetAllResult holds the result of SetAll.Execute().
type SetAllResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *SetAllResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewSetAll returns a new SetAll.
func NewSetAll(k interface{}, v interface{}, sk interface{}) (*SetAll, error) {
	var key []byte
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

	var val []byte
	switch v.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", v)
	case string:
		if len(v.(string)) != 0 {
			var buf bytes.Buffer
			buf.WriteString(v.(string))
			buf.WriteRune('\u0000')
			val = buf.Bytes()
		}
	case []byte:
		val = v.([]byte)
	}
	if val == nil || len(val) == 0 {
		return nil, errors.New("len(val) is zero")
	}

	var skeys [][]byte
	switch sk.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", k)
	case [][]byte:
		skeys = sk.([][]byte)
	case string:
		if len(sk.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(sk.(string))
			buf.WriteRune('\u0000')
			skeys = append(skeys, buf.Bytes())
		}
	case []string:
		skeys = make([][]byte, len(sk.([]string)))
		for _, s := range sk.([]string) {
			if len(s) > 0 {
				var buf bytes.Buffer
				buf.WriteString(s)
				buf.WriteRune('\u0000')
				skeys = append(skeys, buf.Bytes())
			}
		}
	}
	if skeys == nil || len(skeys) == 0 {
		return nil, errors.New("len(skeys) is zero")
	}

	r := &SetAllResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &SetAll{
		key:    key,
		val:    key,
		skeys:  skeys,
		pass:   "",
		expire: 0,
		result: r,
	}
	return c, nil
}

// Execute calls the C.k2hdkc_pm_set_all_wa function.
func (r *SetAll) Execute(s *Session) (bool, error) {

	// zero len(r.val) and zero len(r.skeys) are acceptable though zero len(r.key) is not acceptable.
	if r.key == nil || len(r.key) == 0 || r.val == nil || r.skeys == nil || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.val %v, r.skeys %v, r.result %v", r.key, r.val, r.skeys, r.result)
	}
	pack := make([]C.PK2HDKCKEYPCK, len(r.skeys))
	for i, skey := range r.skeys {
		key := (*C.uchar)(C.CBytes(skey))
		defer C.free(unsafe.Pointer(key))
		length := C.size_t(len(skey))
		pack[i] = &C.K2HKEYPCK{length: length, pkey: key}
	}

	cKey := C.CBytes(r.key)
	defer C.free(unsafe.Pointer(cKey))
	cVal := C.CBytes(r.val)
	defer C.free(unsafe.Pointer(cVal))
	// pass(default is nil) is optional. Go nil is eqaul to C NULL.
	// The pass argment of NULL is acceptable for the k2hdkc C API.
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	r.result.ok = C.k2hdkc_pm_set_all_wa(
		s.handler,
		(*C.uchar)(cKey),
		(C.size_t)(len(r.key)),
		(*C.uchar)(cVal),
		(C.size_t)(len(r.val)),
		pack[0],
		(C.int)(len(pack)),
		cPass,
		expire)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, errors.New("C.k2hdkc_pm_set_all_wa returned false")
	}
	return true, nil
}

// Result returns the pointer of SetAllResult that has the result of Execute method.
func (r *SetAll) Result() *SetAllResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_set_all_wa has been successfully called.
func (r *SetAllResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_set_all_wa in string format.
func (r *SetAllResult) Error() string {
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
