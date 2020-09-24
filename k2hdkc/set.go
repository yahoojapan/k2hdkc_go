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

// Set holds arguments for C.k2hdkc_pm_set_value_wa and a pointer of SetResult.
type Set struct {
	key          []byte
	val          []byte
	rmSubKeyList bool
	pass         string
	expire       int64
	result       *SetResult
}

// String returns a text representation of the object.
func (r *Set) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v]", r.key, r.val, r.rmSubKeyList, r.pass, r.expire, r.result)
}

// SetResult holds the result of Set.Execute().
type SetResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *SetResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewSet returns a new Set.
func NewSet(k interface{}, v interface{}) (*Set, error) {
	// key
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

	// val
	var val []byte
	switch v.(type) {
	default:
		return nil, fmt.Errorf("unsupported val data format %T", v)
	case string:
		if len(v.(string)) > 0 {
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

	// set key & val
	r := &SetResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &Set{
		key:          key,
		val:          val,
		rmSubKeyList: false,
		pass:         "",
		expire:       0,
		result:       r,
	}
	return c, nil
}

// SetRmSubKeyList sets the rmSubKeyList member.
func (r *Set) SetRmSubKeyList(b bool) {
	r.rmSubKeyList = b
}

// SetEncPass sets the pass member.
func (r *Set) SetEncPass(s string) {
	r.pass = s
}

// SetExpire sets the expire member.
func (r *Set) SetExpire(t int64) {
	r.expire = t
}

// Execute calls the C.k2hdkc_pm_set_value_wa function that adds subkey to the k2hdkc cluster.
func (r *Set) Execute(s *Session) (bool, error) {
	// zero r.val is acceptable but r.val must not be nil.
	if r.key == nil || len(r.key) == 0 || r.val == nil || r.result == nil {
		return false, fmt.Errorf("required members nil, r.key %v, r.val %v, r.result %v", r.key, r.val, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	cVal := C.CBytes(r.val)
	defer C.free(cVal)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	r.result.ok = C.k2hdkc_pm_set_value_wa(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)),
		(*C.uchar)(cVal),
		C.size_t(len(r.val)),
		C._Bool(r.rmSubKeyList),
		cPass,
		expire)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, errors.New("C.k2hdkc_pm_set_value_wa returned false")
	}
	return true, nil
}

// Result returns the pointer of SetResult that has the result of Execute method.
func (r *Set) Result() *SetResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_set_value_wa has been successfully called.
func (r *SetResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_set_value_wa in string format.
func (r *SetResult) Error() string {
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
