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

// CasGet holds arguments for C.k2hdkc_pm_cas{8,16,32,64}_get and a pointer of CasGetResult.
// bool k2hdkc_pm_cas64_get_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, const char* encpass, uint64_t* pval)
type CasGet struct {
	key    []byte
	pass   string
	vlen   uint8 // value len of 8, 16, 32 or 64.
	result *CasGetResult
}

// String returns a text representation of the object.
func (r *CasGet) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v]", r.key, r.pass, r.vlen, r.result)
}

// CasGetResult holds the result of CasGet.Execute().
type CasGetResult struct {
	val        []byte
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *CasGetResult) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v]", r.val, r.ok, r.resCode, r.subResCode)
}

// NewCasGetWithCasType returns a new CasGet.
func NewCasGetWithCasType(k interface{}, ct CasType) (*CasGet, error) {
	// cas type
	var vl uint8
	if ct == CasType8 {
		vl = 8
	} else if ct == CasType16 {
		vl = 16
	} else if ct == CasType32 {
		vl = 32
	} else if ct == CasType64 {
		vl = 64
	} else {
		return nil, fmt.Errorf("ct %v must be any of CasType{8,16,32,64}", ct)
	}

	c, err := NewCasGet(k)
	if err == nil {
		return nil, fmt.Errorf("NewCasGet(%q) = (nil, %v)", k, err)
	}
	c.SetValueLen(vl)
	return c, nil
}

// NewCasGet returns a new CasGet.
func NewCasGet(k interface{}) (*CasGet, error) {
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

	r := &CasGetResult{
		val:        nil,
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &CasGet{
		key:    key,
		pass:   "",
		vlen:   (uint8)(CasType32),
		result: r,
	}
	return c, nil
}

// SetEncPass sets the pass member.
func (r *CasGet) SetEncPass(s string) {
	r.pass = s
}

// SetValueLen sets the vlen member.
func (r *CasGet) SetValueLen(vl uint8) {
	r.vlen = vl
}

// Execute calls the C.k2hdkc_pm_cas{8,16,32,64}_get_wa function.
func (r *CasGet) Execute(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.result %v", r.key, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	switch r.vlen {
	default:
		return false, fmt.Errorf("unsupported data format %T", r.vlen)
	case 8:
		{
			// bool k2hdkc_pm_cas8_get_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, const char* encpass, uint8_t* pval)
			var cVal C.uint8_t
			r.result.ok = C.k2hdkc_pm_cas8_get_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cPass,
				&cVal)
			// See https://golang.org/pkg/unsafe/#Pointer
			defer C.free(unsafe.Pointer(uintptr(unsafe.Pointer(&cVal))))

			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas8_get_wa returned false")
			}
			r.result.val = make([]byte, 1)
			r.result.val[0] = (uint8)(cVal)
		}
	case 16:
		{
			var cVal C.uint16_t
			r.result.ok = C.k2hdkc_pm_cas16_get_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cPass,
				&cVal)
			defer C.free(unsafe.Pointer(uintptr(unsafe.Pointer(&cVal))))
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas16_get_wa returned false")
			}
			r.result.val = make([]byte, 2)
			for i := 0; i < 2; i++ {
				r.result.val[i] = (uint8)(cVal >> (uint16)(8*i)) // OK
			}
		}
	case 32:
		{
			var cVal C.uint32_t
			r.result.ok = C.k2hdkc_pm_cas32_get_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cPass,
				&cVal)
			defer C.free(unsafe.Pointer(uintptr(unsafe.Pointer(&cVal))))
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas32_get_wa returned false")
			}
			r.result.val = make([]byte, 4)
			for i := 0; i < 4; i++ {
				r.result.val[i] = (uint8)(cVal >> (uint32)(8*i)) // OK
			}
		}
	case 64:
		{
			var cVal C.uint64_t
			r.result.ok = C.k2hdkc_pm_cas64_get_wa(
				s.handler,
				(*C.uchar)(cKey),
				C.size_t(len(r.key)),
				cPass,
				&cVal)
			defer C.free(unsafe.Pointer(uintptr(unsafe.Pointer(&cVal))))
			r.result.resCode = C.k2hdkc_get_res_code(s.handler)
			r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
			if !r.result.ok {
				return false, errors.New("C.k2hdkc_pm_cas64_get_wa returned false")
			}
			r.result.val = make([]byte, 8)
			for i := 0; i < 8; i++ {
				r.result.val[i] = (uint8)(cVal >> (uint64)(8*i)) // OK
			}
		}
	}
	return true, nil
}

// Result returns the pointer of CasGetResult that has the result of Execute method.
func (r *CasGet) Result() *CasGetResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bytes returns the C.k2hdkc_pm_cas{8,16,32,64}_get_wa response in binary format.
func (r *CasGetResult) Bytes() []byte {
	if r.val != nil {
		return r.val
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_cas{8,16,32,64}_get_wa has been successfully called.
func (r *CasGetResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_cas{8,16,32,64}_get_wa in string format.
func (r *CasGetResult) Error() string {
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
