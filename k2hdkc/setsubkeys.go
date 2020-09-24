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

// SetSubKeys holds arguments for C.k2hdkc_pm_set_subkeys and a pointer of SetSubKeysResult.
type SetSubKeys struct {
	key    []byte
	skeys  [][]byte
	result *SetSubKeysResult
}

// String returns a text representation of the object.
func (r *SetSubKeys) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.key, r.skeys, r.result)
}

// SetSubKeysResult holds the result of SetSubKeys.Execute().
type SetSubKeysResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *SetSubKeysResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewSetSubKeys returns a new SetSubKeys.
func NewSetSubKeys(k interface{}, sk interface{}) (*SetSubKeys, error) {
	// key
	var key []byte
	switch k.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", k)
	case string:
		if len(k.(string)) != 0 {
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

	// subkeys
	var skeys [][]byte
	switch sk.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", k)
	case [][]byte:
		skeys = sk.([][]byte)
	case string:
		if len(sk.(string)) != 0 {
			var buf bytes.Buffer
			buf.WriteString(sk.(string))
			buf.WriteRune('\u0000')
			skeys = append(skeys, buf.Bytes())
		}
	case []string:
		for _, s := range sk.([]string) {
			if len(s) != 0 {
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

	r := &SetSubKeysResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &SetSubKeys{
		key:    key,
		skeys:  skeys,
		result: r,
	}
	return c, nil
}

// Execute calls C.k2hdkc_pm_set_subkeys that set a subkey.
func (r *SetSubKeys) Execute(s *Session) (bool, error) {

	// r.key and r.skeys are must. empty r.skeys which means subkeys is acceptable though empty r.key is not.
	if r.key == nil || len(r.key) == 0 || r.skeys == nil || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.skeys %v, r.result %v", r.key, r.skeys, r.result)
	}

	cKey := C.CBytes(r.key)
	defer C.free(unsafe.Pointer(cKey))

	pack := make([]C.PK2HDKCKEYPCK, len(r.skeys))
	for i, skey := range r.skeys {
		key := (*C.uchar)(C.CBytes(skey)) // key will be freed after calling bC.k2hdkc_pm_set_subkeys
		length := C.size_t(len(skey))
		pack[i] = &C.K2HKEYPCK{length: length, pkey: key}
	}
	r.result.ok = C.k2hdkc_pm_set_subkeys(
		s.handler,
		(*C.uchar)(cKey),
		(C.size_t)(len(r.key)),
		pack[0],
		(C.int)(len(pack)))
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	// frees keys in keypack
	for i := range r.skeys {
		key := pack[i].pkey
		defer C.free(unsafe.Pointer(key))
	}

	if r.result.ok == false {
		return false, fmt.Errorf("C.k2hdkc_pm_set_subkeys %v", r.result.ok)
	}
	return true, nil
}

// Result returns the result of Execute method.
func (r *SetSubKeys) Result() *SetSubKeysResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns whether C.k2hdkc_pm_set_subkeys returned success or not.
func (r *SetSubKeysResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_set_subkeys() in string format.
func (r *SetSubKeysResult) Error() string {
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
