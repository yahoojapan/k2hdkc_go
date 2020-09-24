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
	"strconv"
	"unsafe"
)

// GetAttrs holds arguments for C.k2hdkc_pm_get_attrs and a pointer of GetAttrsResult.
type GetAttrs struct {
	key    []byte
	result *GetAttrsResult
}

// String returns a text representation of the object.
func (r *GetAttrs) String() string {
	return fmt.Sprintf("[%v, %v]", r.key, r.result)
}

// Attr holds attribute names and values.
type Attr struct {
	key []byte
	val []byte
}

// String returns a text representation of the object.
func (r *Attr) String() string {
	return fmt.Sprintf("[%v, %v]", r.key, r.val)
}

// GetAttrsResult holds the result of GetAttrs.Execute().
type GetAttrsResult struct {
	attrs      []*Attr
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
//func (r *GetAttrsResult) String() string {
//	return fmt.Sprintf("[%v, %v, %v]", r.attrs, r.ok, r.resCode, r.subResCode)
//}

// NewGetAttrs returns a new GetAttrs.
func NewGetAttrs(k interface{}) (*GetAttrs, error) {
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

	result := &GetAttrsResult{
		attrs: nil,
	}
	c := &GetAttrs{
		key:    key,
		result: result,
	}
	return c, nil
}

// Execute calls the C.k2hdkc_pm_get_attrs function that get key attributes from the k2hdkc cluster.
func (r *GetAttrs) Execute(s *Session) (bool, error) {
	if r.key == nil || len(r.key) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.result %v", r.key, r.result)
	}
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	var attrpack C.PK2HDKCATTRPCK
	var attrpackLen C.int
	r.result.ok = C.k2hdkc_pm_get_attrs(
		s.handler,
		(*C.uchar)(cKey),
		C.size_t(len(r.key)),
		&attrpack,
		&attrpackLen,
	)
	defer C.dkc_free_attrpack(attrpack, attrpackLen)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
	if r.result.ok == false {
		return false, fmt.Errorf("C.k2hdkc_pm_get_subkeys() = %v", r.result.ok)
	}
	if attrpackLen == 0 {
		return true, nil
	}
	/*
	   typedef struct k2h_attr_pack{
	   	unsigned char*pkey;
	   	size_t keylength;
	   	unsigned char*pval;
	   	size_t vallength;
	   }K2HATTRPCK, *PK2HATTRPCK;
	*/
	// See https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	var theCArray C.PK2HDKCATTRPCK = attrpack
	length := (int)(attrpackLen)
	cslice := (*[1 << 30]C.K2HATTRPCK)(unsafe.Pointer(theCArray))[:length:length]
	r.result.attrs = make([]*Attr, length) // copy
	for i, data := range cslice {
		akey := C.GoBytes(unsafe.Pointer(data.pkey), (C.int)(data.keylength))
		aval := C.GoBytes(unsafe.Pointer(data.pval), (C.int)(data.vallength))
		r.result.attrs[i] = &Attr{akey, aval}
	}
	return true, nil
}

// Result returns the pointer of GetAttrs that has the result of Execute method.
func (r *GetAttrs) Result() *GetAttrsResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bytes returns the C.k2hdkc_pm_get_attrs response in binary format.
func (r *GetAttrsResult) Bytes() []*Attr {
	return r.attrs
}

// getUnixTime gets first 8bytes as uint64
func getUnixTime(val []byte) (uint64, error) {
	if len(val) < 8 {
		return 0, errors.New("len(val) < 8, want len(val) => 8")
	}
	unixTime := ((uint64)(val[0]) | // 1st
		((uint64)(val[1]) << 8) | // 2nd
		((uint64)(val[2]) << 16) | // 3rd
		((uint64)(val[3]) << 24) | // 4th
		((uint64)(val[4]) << 32) | // 5th
		((uint64)(val[5]) << 40) | // 6th
		((uint64)(val[6]) << 48) | // 7th
		((uint64)(val[7]) << 56)) // 8th
	return unixTime, nil
}

// String returns the C.k2hdkc_pm_get_attrs response in string format.
func (r *GetAttrsResult) String() map[string]string {
	if r.attrs == nil {
		return nil
	}
	attrs := make(map[string]string, len(r.attrs))
	for _, attr := range r.attrs {
		key := string(attr.key)
		if string(attr.key[len(attr.key)-1]) == string("\u0000") {
			key = string(attr.key[:len(attr.key)-1])
		}
		val := string(attr.val)
		if key == "expire" || key == "mtime" {
			if unixTime, err := getUnixTime(attr.val); err == nil {
				val = strconv.FormatUint(unixTime, 10)
			}
		}
		attrs[key] = val
	}
	return attrs
}

// Bool returns true if C.k2hdkc_pm_get_attrs has been successfully called.
func (r *GetAttrsResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_get_attrs in string format.
func (r *GetAttrsResult) Error() string {
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
