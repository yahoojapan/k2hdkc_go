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

// RemoveSubKey holds arguments C.k2hdkc_pm_remove_subkey for and a pointer of RemoveSubKeyResult.
type RemoveSubKey struct {
	key    []byte
	skey   []byte
	nest   bool
	result *RemoveSubKeyResult
}

// String returns a text representation of the object.
func (r *RemoveSubKey) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v]", r.key, r.skey, r.nest, r.result)
}

// RemoveSubKeyResult holds the result of RemoveSubKey.Execute().
type RemoveSubKeyResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *RemoveSubKeyResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewRemoveSubKey returns a new RemoveSubKey.
func NewRemoveSubKey(k interface{}, sk interface{}) (*RemoveSubKey, error) {
	var key []byte
	var skey []byte

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

	switch sk.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", k)
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

	r := &RemoveSubKeyResult{
		ok:         false, // default is false.
		resCode:    0,
		subResCode: 0,
	}
	c := &RemoveSubKey{
		key:    key,
		skey:   skey,
		result: r,
	}
	return c, nil
}

// Execute calls C.k2hdkc_pm_remove_subkey that remove a subkey.
func (r *RemoveSubKey) Execute(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 || r.skey == nil || len(r.skey) == 0 || r.result == nil {
		return false, fmt.Errorf("required members nil, r.key %v, r.skey %v, r.result %v", r.key, r.skey, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(unsafe.Pointer(cKey))
	cSkey := C.CBytes(r.skey)
	defer C.free(unsafe.Pointer(cSkey))

	r.result.ok = C.k2hdkc_pm_remove_subkey(
		s.handler,
		(*C.uchar)(cKey),
		(C.size_t)(len(r.key)),
		(*C.uchar)(cSkey),
		(C.size_t)(len(r.skey)),
		(C._Bool)(r.nest))
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, fmt.Errorf("C.k2hdkc_pm_remove_subkey() = %v", r.result.ok)
	}
	return true, nil
}

// Result returns the pointer of RemoveSubKeyResult that has the result of Execute method.
func (r *RemoveSubKey) Result() *RemoveSubKeyResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_remove_subkey has been successfully called.
func (r *RemoveSubKeyResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_remove_subkey() in string format.
func (r *RemoveSubKeyResult) Error() string {
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
