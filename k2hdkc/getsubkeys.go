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

// GetSubKeys holds arguments for C.k2hdkc_pm_get_subkeys and a pointer of GetSubKeysResult.
type GetSubKeys struct {
	key    []byte
	result *GetSubKeysResult
}

// String returns a text representation of the object.
func (r *GetSubKeys) String() string {
	return fmt.Sprintf("[%v, %v]", r.key, r.result)
}

// GetSubKeysResult holds the result of GetSubKeys.Execute().
type GetSubKeysResult struct {
	skeys      [][]byte
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
//func (r *GetSubKeysResult) String() string {
//	return fmt.Sprintf("[%v, %v, %v, %v]", r.subkeys, r.ok, r.resCode, r.subResCode)
//}

// NewGetSubKeys returns a new GetSubKeys.
func NewGetSubKeys(k interface{}) (*GetSubKeys, error) {
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

	r := &GetSubKeysResult{
		skeys:      nil,
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &GetSubKeys{
		key:    key,
		result: r,
	}
	return c, nil
}

// Execute calls the C.k2hdkc_pm_get_subkeys function that gets subkey to the k2hdkc cluster.
func (r *GetSubKeys) Execute(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 || r.result == nil {
		return false, fmt.Errorf("required members nil, r.key %v, r.result %v", r.key, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	var keypack C.PK2HDKCKEYPCK
	var keypackLen C.int

	r.result.ok = C.k2hdkc_pm_get_subkeys(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)),
		&keypack,
		&keypackLen,
	)
	defer C.dkc_free_keypack(keypack, keypackLen)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, fmt.Errorf("C.k2hdkc_pm_get_subkeys() = %v", r.result.ok)
	}

	if keypackLen == 0 {
		return true, nil
	}

	// See https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	var theCArray C.PK2HDKCKEYPCK = keypack
	length := (int)(keypackLen)
	cslice := (*[1 << 30]C.K2HKEYPCK)(unsafe.Pointer(theCArray))[:length:length]
	r.result.skeys = make([][]byte, length) // copy
	for i, data := range cslice {
		sk := C.GoBytes(unsafe.Pointer(data.pkey), (C.int)(data.length))
		r.result.skeys[i] = sk
	}
	return true, nil
}

// Result returns the pointer of GetSubKeysResult that has the result of Execute method.
func (r *GetSubKeys) Result() *GetSubKeysResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bytes returns the C.k2hdkc_pm_get_subkeys() response in binary format.
func (r *GetSubKeysResult) Bytes() [][]byte {
	return r.skeys
}

// String returns the C.k2hdkc_pm_get_subkeys() response in string format.
func (r *GetSubKeysResult) String() []string {
	slice := make([]string, len(r.skeys))
	for i, s := range r.skeys {
		valLen := len(s)
		if valLen > 0 {
			slice[i] = string(s[:valLen-1]) // make a new string without the null termination
		} else {
			slice[i] = ""
		}
	}
	return slice
}

// Bool returns true if C.k2hdkc_pm_get_subkeys() has been successfully called.
func (r *GetSubKeysResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_get_subkeys() in string format.
func (r *GetSubKeysResult) Error() string {
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
