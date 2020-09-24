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

// CasInit holds arguments for C.k2hdkc_pm_cas{8,16,32,64}_init and a pointer of CasInitResult.
type CasInit struct {
	key    []byte
	val    []byte
	pass   string
	expire int64
	result *CasInitResult
}

// String returns a text representation of the object.
func (r *CasInit) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v]", r.key, r.val, r.pass, r.expire, r.result)
}

// CasInitResult holds the result of CasInit.Execute().
type CasInitResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *CasInitResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewCasInitWithValue returns a new CasInit
func NewCasInitWithValue(k interface{}, v interface{}) (*CasInit, error) {
	c, err := NewCasInit(k)

	if err == nil {
		var val []byte
		switch v.(type) {
		default:
			return nil, fmt.Errorf("unsupported val data format %T", v)
		case uint8:
			val = make([]byte, 1)
			val[0] = v.(uint8)
		case uint16:
			val = make([]byte, 2)
			for i := 0; i < 2; i++ {
				val[i] = (uint8)(v.(uint16) >> (uint16)(8*i)) // OK
			}
		case uint32:
			val = make([]byte, 4)
			for i := 0; i < 4; i++ {
				val[i] = (uint8)(v.(uint32) >> (uint32)(8*i)) // OK
			}
		case uint64:
			val = make([]byte, 8)
			for i := 0; i < 8; i++ {
				val[i] = (uint8)(v.(uint64) >> (uint64)(8*i)) // OK
			}
		case []byte:
			val = v.([]byte)
		}
		if val == nil || len(val) == 0 {
			return nil, errors.New("val is nil or len(val) is zero")
		}
		c.SetValue(val)
		return c, nil
	}
	return nil, fmt.Errorf("NewCasInit(%q, %q) initialize error %v", k, v, err)
}

// NewCasInit returns a new CasInit.
func NewCasInit(k interface{}) (cmd *CasInit, err error) {
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
	r := &CasInitResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	val := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	c := &CasInit{
		key:    key,
		val:    val,
		pass:   "",
		expire: 0,
		result: r,
	}
	return c, nil
}

// SetValue sets the val member.
func (r *CasInit) SetValue(v []byte) {
	r.val = v
}

// SetEncPass sets the pass member.
func (r *CasInit) SetEncPass(s string) {
	r.pass = s
}

// SetExpire sets the expire member.
func (r *CasInit) SetExpire(t int64) {
	r.expire = t
}

// Execute calls the C.k2hdkc_pm_cas{8,16,32,64}_init_wa function that initialize the key in the k2hdkc cluster.
func (r *CasInit) Execute(s *Session) (bool, error) {
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
	switch len(r.val) {
	default:
		return false, fmt.Errorf("unsupported data format %T", r.val)
	case 1:
		{
			// bool k2hdkc_pm_cas8_init_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint8_t val, const char* encpass, const time_t* expire)
			cVal := (C.uint8_t)((uint8)(r.val[0]))
			r.result.ok = C.k2hdkc_pm_cas8_init_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cVal,
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas8_init_wa returned false")
			}
		}
	case 2:
		{
			// bool k2hdkc_pm_cas16_init_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint16_t val, const char* encpass, const time_t* expire)
			cVal := (C.uint16_t)(
				((uint16)(r.val[0])) | // first
					((uint16)(r.val[1]) << 8))
			r.result.ok = C.k2hdkc_pm_cas16_init_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				(C.uint16_t)(cVal),
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas16_init_wa returned false")
			}
		}
	case 4:
		{
			// bool k2hdkc_pm_cas64_init_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint32_t val, const char* encpass, const time_t* expire)
			cVal := (C.uint32_t)(
				((uint32)(r.val[0])) | // first
					((uint32)(r.val[1]) << 8) | // second
					((uint32)(r.val[2]) << 16) | // 3rd
					((uint32)(r.val[3]) << 24)) // 4th
			r.result.ok = C.k2hdkc_pm_cas32_init_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				(C.uint32_t)(cVal),
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas32_init_wa returned false")
			}
		}
	case 8:
		{
			// bool k2hdkc_pm_cas64_init_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint64_t val, const char* encpass, const time_t* expire)
			cVal := (C.uint64_t)(
				((uint64)(r.val[0])) | // 1st
					((uint64)(r.val[1]) << 8) | // 2nd
					((uint64)(r.val[2]) << 16) | // 3rd
					((uint64)(r.val[3]) << 24) | // 4th
					((uint64)(r.val[4]) << 32) | // 5th
					((uint64)(r.val[5]) << 40) | // 6th
					((uint64)(r.val[6]) << 48) | // 7th
					((uint64)(r.val[7]) << 56)) // 8th
			r.result.ok = C.k2hdkc_pm_cas64_init_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				(C.uint64_t)(cVal),
				cPass,
				expire)
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas64_init_wa returned false")
			}
		}
	}
	return true, nil
}

// Result returns the pointer of CasInitResult that has the result of Execute method.
func (r *CasInit) Result() *CasInitResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_cas{8,16,32,64}_init_wa has been successfully called.
func (r *CasInitResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_cas{8,16,32,64}_init_wa in string format.
func (r *CasInitResult) Error() string {
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
