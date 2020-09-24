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

// CasIncDec holds arguments for C.k2hdkc_pm_cas{8,16,32,64}_{in,de}crement and a pointer of CasIncDecResult.
type CasIncDec struct {
	key    []byte
	pass   string
	expire int64
	incr   bool
	result *CasIncDecResult
}

// String returns a text representation of the object.
func (r *CasIncDec) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v]", r.key, r.pass, r.expire, r.incr, r.result)
}

// CasIncDecResult holds the result of CasIncDec.Execute().
type CasIncDecResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *CasIncDecResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewCasIncDec returns a new CasIncDec.
func NewCasIncDec(k interface{}, i bool) (*CasIncDec, error) {
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

	// set key & val
	r := &CasIncDecResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &CasIncDec{
		key:    key,
		pass:   "",
		expire: 0,
		incr:   i,
		result: r,
	}
	return c, nil
}

// SetEncPass sets the pass member.
func (r *CasIncDec) SetEncPass(s string) {
	r.pass = s
}

// SetExpire sets the expire member.
func (r *CasIncDec) SetExpire(t int64) {
	r.expire = t
}

// Increment calls the C.k2hdkc_pm_cas_increment_wa function.
func (r *CasIncDec) Increment(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.result %v", r.key, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t // default
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}

	// bool k2hdkc_pm_cas_increment_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, const char* encpass, const time_t* expire)
	r.result.ok = C.k2hdkc_pm_cas_increment_wa(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)),
		cPass,
		expire)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if !r.result.ok {
		return false, errors.New("C.k2hdkc_pm_cas_increment_wa returned false")
	}
	return true, nil
}

// Decrement calls the C.k2hdkc_pm_cas_decrement_wa function.
func (r *CasIncDec) Decrement(s *Session) (bool, error) {
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t // default
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	// bool k2hdkc_pm_cas_decrement_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, const char* encpass, const time_t* expire)
	r.result.ok = C.k2hdkc_pm_cas_decrement_wa(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)),
		cPass,
		expire)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if !r.result.ok {
		return false, errors.New("C.k2hdkc_pm_cas_decrement_wa returned false")
	}
	return true, nil
}

// Execute calls the C.k2hdkc_pm_cas_{in,de}crement_wa function.
func (r *CasIncDec) Execute(s *Session) (bool, error) {
	if r.incr {
		return r.Increment(s)
	}
	return r.Decrement(s)
}

// Result returns the pointer of CasIncDecResult that has the result of Execute method.
func (r *CasIncDec) Result() *CasIncDecResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_cas_{in,de}crement_wa has been successfully called.
func (r *CasIncDecResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_cas_{in,de}crement_wa in string format.
func (r *CasIncDecResult) Error() string {
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
