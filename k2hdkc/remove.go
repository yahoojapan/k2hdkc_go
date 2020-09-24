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
)

// Remove holds arguments for C.k2hdkc_pm_remove and a pointer of RemoveResult.
type Remove struct {
	key    []byte
	result *RemoveResult
}

// String returns a text representation of the object.
func (r *Remove) String() string {
	return fmt.Sprintf("[%v, %v]", r.key, r.result)
}

// RemoveResult holds the result of RemoveResult.Execute().
type RemoveResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *RemoveResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewRemove returns the pointer to a Command struct.
func NewRemove(k interface{}) (cmd *Remove, err error) {
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

	r := &RemoveResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &Remove{
		key:    key,
		result: r,
	}
	return c, nil
}

// Execute calls the C.k2hdkc_pm_remove function which is the lowest C API.
func (r *Remove) Execute(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 {
		return false, fmt.Errorf("r.key is nil or zero length %v", r.key)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)

	// bool k2hdkc_pm_remove(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength)
	r.result.ok = C.k2hdkc_pm_remove(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)))
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, errors.New("C.k2hdkc_pm_remove returned false")
	}
	return true, nil
}

// Result returns the pointer of RemoveResult that has the result of Execute method.
func (r *Remove) Result() *RemoveResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_remove has been successfully called.
func (r *RemoveResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_remove in string format.
func (r *RemoveResult) Error() string {
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
