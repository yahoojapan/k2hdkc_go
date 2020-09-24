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

// Get holds arguments for C.k2hdkc_pm_get_value_wp and a pointer of GetResult.
type Get struct {
	key    []byte
	pass   string
	result *GetResult
}

// String returns a text representation of the object.
func (r *Get) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.key, r.pass, r.result)
}

// GetResult holds the result of Get.Execute().
type GetResult struct {
	val        []byte
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
//func (r *GetResult) String() string {
//	return fmt.Sprintf("[%v, %v, %v, %v]", r.val, r.ok, r.resCode, r.subResCode)
//}

// NewGet returns the pointer to a Command struct.
func NewGet(k interface{}) (*Get, error) {
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

	r := &GetResult{
		val:        []byte{},
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &Get{
		key:    key,
		pass:   "",
		result: r,
	}
	return c, nil
}

// SetEncPass set the password.
func (r *Get) SetEncPass(s string) {
	r.pass = s
}

// Execute calls the C.k2hdkc_pm_get_value_wp function which is the lowest C API.
func (r *Get) Execute(s *Session) (bool, error) {
	// r.key is a must.
	if r.key == nil || len(r.key) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.result %v", r.key, r.result)
	}
	cKey := C.CBytes(r.key) // func C.CBytes([]byte) unsafe.Pointer
	defer C.free(cKey)
	cPass := C.CString(r.pass) // func C.CString(string) *C.char
	defer C.free(unsafe.Pointer(cPass))

	var cRetValue *C.uchar // value:(*main._Ctype_char)(nil) type:*main._Ctype_char
	var valLen C.size_t    // valLen value:0x0 type:main._Ctype_size_t
	r.result.ok = C.k2hdkc_pm_get_value_wp(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)),
		cPass,
		&cRetValue,
		&valLen)
	defer C.free(unsafe.Pointer(cRetValue))
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp() = %v", r.result.ok)
	}
	r.result.val = C.GoBytes(unsafe.Pointer(cRetValue), C.int(valLen))
	return true, nil
}

// Result returns the pointer of GetResult that has the result of Execute method.
func (r *Get) Result() *GetResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bytes returns the C.k2hdkc_pm_get_value_wp response in binary format.
func (r *GetResult) Bytes() []byte {
	return r.val
}

// String returns the C.k2hdkc_pm_get_value_wp response in text format.
func (r *GetResult) String() string {
	valLen := len(r.val)
	if valLen > 0 {
		return string(r.val[:valLen-1]) // make a new string without the null termination
	}
	return ""
}

// Bool returns true if C.k2hdkc_pm_get_value_wp has been successfully called.
func (r *GetResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_get_value_wp in string format.
func (r *GetResult) Error() string {
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
