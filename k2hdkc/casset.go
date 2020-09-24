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

// CasSet holds arguments for C.k2hdkc_pm_cas{8,16,32,64}_set and a pointer of CasSetResult.
// bool k2hdkc_pm_cas64_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint64_t oldval, uint64_t newval, const char* encpass, const time_t* expire)
type CasSet struct {
	key    []byte
	old    []byte
	new    []byte
	pass   string
	expire int64
	result *CasSetResult
}

// String returns a text representation of the object.
func (r *CasSet) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v]", r.key, r.old, r.new, r.pass, r.expire, r.result)
}

// CasSetResult holds the result of CasSet.Execute().
type CasSetResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *CasSetResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewCasSet returns a new CasSet.
func NewCasSet(k interface{}, o interface{}, n interface{}) (*CasSet, error) {
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

	// old
	var old []byte
	switch o.(type) {
	default:
		return nil, fmt.Errorf("unsupported val data format %T", o)
	case uint8:
		old = make([]byte, 1)
		old[0] = o.(uint8)
	case uint16:
		old = make([]byte, 2)
		for i := 0; i < 2; i++ {
			old[i] = (uint8)(o.(uint16) >> (uint16)(8*i)) // OK
		}
	case uint32:
		old = make([]byte, 4)
		for i := 0; i < 4; i++ {
			old[i] = (uint8)(o.(uint32) >> (uint32)(8*i)) // OK
		}
	case uint64:
		old = make([]byte, 8)
		for i := 0; i < 8; i++ {
			old[i] = (uint8)(o.(uint64) >> (uint64)(8*i)) // OK
		}
	case []byte:
		old = o.([]byte)
	}
	if old == nil || len(old) == 0 {
		return nil, errors.New("old is nil or len(old) is zero")
	}

	// new
	var new []byte
	switch n.(type) {
	default:
		return nil, fmt.Errorf("unsupported val data format %T", n)
	case uint8:
		new = make([]byte, 1)
		new[0] = n.(uint8)
	case uint16:
		new = make([]byte, 2)
		for i := 0; i < 2; i++ {
			new[i] = (uint8)(n.(uint16) >> (uint16)(8*i)) // OK
		}
	case uint32:
		new = make([]byte, 4)
		for i := 0; i < 4; i++ {
			new[i] = (uint8)(n.(uint32) >> (uint32)(8*i)) // OK
		}
	case uint64:
		new = make([]byte, 8)
		for i := 0; i < 8; i++ {
			new[i] = (uint8)(n.(uint64) >> (uint64)(8*i)) // OK
		}
	case []byte:
		new = n.([]byte)
	}
	if new == nil || len(new) == 0 {
		return nil, errors.New("new is nil or len(new) is zero")
	}

	// length of old and new must be same.
	if len(old) != len(new) {
		return nil, fmt.Errorf("len(old) %v len(new) %v must be same", len(old), len(new))
	}

	// set key & val
	r := &CasSetResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &CasSet{
		key:    key,
		old:    old,
		new:    new,
		pass:   "",
		expire: 0,
		result: r,
	}
	return c, nil
}

// SetEncPass sets the pass member.
func (r *CasSet) SetEncPass(s string) {
	r.pass = s
}

// SetExpire sets the expire member.
func (r *CasSet) SetExpire(t int64) {
	r.expire = t
}

// Execute calls the C.k2hdkc_pm_cas{8,16,32,64}_set_wa function that swap the key in the k2hdkc cluster.
func (r *CasSet) Execute(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.result %v", r.key, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	// length of old and new must be same.
	if len(r.old) != len(r.new) {
		return false, fmt.Errorf("len(r.old) %v len(r.new) %v must be same", len(r.old), len(r.new))
	}

	switch len(r.old) {
	default:
		return false, fmt.Errorf("unsupported data format %T", r.old)
	case 1:
		{
			// bool k2hdkc_pm_cas8_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint8_t oldval, uint8_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint8_t)((uint8)(r.old[0]))
			cNew := (C.uint8_t)((uint8)(r.new[0]))
			r.result.ok = C.k2hdkc_pm_cas8_set_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cOld,
				cNew,
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas8_set_wa returned")
			}
		}
	case 2:
		{
			// bool k2hdkc_pm_cas16_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint16_t oldval, uint16_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint16_t)(
				((uint16)(r.old[0])) | // first
					((uint16)(r.old[1]) << 8))
			cNew := (C.uint16_t)(
				((uint16)(r.new[0])) | // first
					((uint16)(r.new[1]) << 8))
			r.result.ok = C.k2hdkc_pm_cas16_set_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cOld,
				cNew,
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas16_set_wa returned")
			}
		}
	case 4:
		{
			// bool k2hdkc_pm_cas32_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint32_t oldval, uint32_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint32_t)(
				((uint32)(r.old[0])) | // first
					((uint32)(r.old[1]) << 8) | // second
					((uint32)(r.old[2]) << 16) | // 3rd
					((uint32)(r.old[3]) << 24)) // 4th
			cNew := (C.uint32_t)(
				((uint32)(r.new[0])) | // first
					((uint32)(r.new[1]) << 8) | // second
					((uint32)(r.new[2]) << 16) | // 3rd
					((uint32)(r.new[3]) << 24)) // 4th
			r.result.ok = C.k2hdkc_pm_cas32_set_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cOld,
				cNew,
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas32_set_wa returned")
			}
		}
	case 8:
		{
			// bool k2hdkc_pm_cas64_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint64_t oldval, uint64_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint64_t)(
				((uint64)(r.old[0])) | // 1st
					((uint64)(r.old[1]) << 8) | // 2nd
					((uint64)(r.old[2]) << 16) | // 3rd
					((uint64)(r.old[3]) << 24) | // 4th
					((uint64)(r.old[4]) << 32) | // 5th
					((uint64)(r.old[5]) << 40) | // 6th
					((uint64)(r.old[6]) << 48) | // 7th
					((uint64)(r.old[7]) << 56)) // 8th
			cNew := (C.uint64_t)(
				((uint64)(r.new[0])) | // 1st
					((uint64)(r.new[1]) << 8) | // 2nd
					((uint64)(r.new[2]) << 16) | // 3rd
					((uint64)(r.new[3]) << 24) | // 4th
					((uint64)(r.new[4]) << 32) | // 5th
					((uint64)(r.new[5]) << 40) | // 6th
					((uint64)(r.new[6]) << 48) | // 7th
					((uint64)(r.new[7]) << 56)) // 8th
			r.result.ok = C.k2hdkc_pm_cas64_set_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cOld,
				cNew,
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas64_set_wa returned")
			}
		}
	}
	return true, nil
}

// Result returns the pointer of CasSetResult that has the result of Execute method.
func (r *CasSet) Result() *CasSetResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_cas{8,16,32,64}_set_wa has been successfully called.
func (r *CasSetResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_cas{8,16,32,64}_set_wa in string format.
func (r *CasSetResult) Error() string {
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
