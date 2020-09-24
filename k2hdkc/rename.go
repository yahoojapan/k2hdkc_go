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

// Rename holds arguments for C.k2hdkc_pm_rename_with_parent_wa and a pointer of RenameResult.
type Rename struct {
	oldKey    []byte
	newKey    []byte
	parentKey []byte
	attr      bool
	pass      string
	expire    int64
	result    *RenameResult
}

// String returns a text representation of the object.
func (r *Rename) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v, %v]", r.oldKey, r.newKey, r.parentKey, r.attr, r.pass, r.expire, r.result)
}

// RenameResult holds the result of Rename.Execute().
type RenameResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *RenameResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewRename returns a Rename.
func NewRename(o interface{}, n interface{}) (cmd *Rename, err error) {
	// old key
	var oldKey []byte
	switch o.(type) {
	default:
		return nil, fmt.Errorf("unsupported key data format %T", o)
	case string:
		if len(o.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(o.(string))
			buf.WriteRune('\u0000')
			oldKey = buf.Bytes()
		}
	case []byte:
		oldKey = o.([]byte)
	}
	if oldKey == nil || len(oldKey) == 0 {
		return nil, errors.New("len(oldKey) is zero")
	}

	// new key
	var newKey []byte
	switch n.(type) {
	default:
		return nil, fmt.Errorf("unsupported val data format %T", n)
	case string:
		if len(n.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(n.(string))
			buf.WriteRune('\u0000')
			newKey = buf.Bytes()
		}
	case []byte:
		newKey = n.([]byte)
	}
	if newKey == nil || len(newKey) == 0 {
		return nil, errors.New("len(newKey) is zero")
	}

	// set key & val
	r := &RenameResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &Rename{
		oldKey:    oldKey,
		newKey:    newKey,
		parentKey: nil,
		attr:      defaultCheckAttr,
		pass:      "",
		expire:    0,
		result:    r,
	}
	return c, nil
}

// SetParentKey sets the parentKey member.
func (r *Rename) SetParentKey(p interface{}) (bool, error) {
	var parentKey []byte
	switch p.(type) {
	default:
		return false, fmt.Errorf("unsupported val data format %T, want string or []byte", p)
	case string:
		if len(p.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(p.(string))
			buf.WriteRune('\u0000')
			parentKey = buf.Bytes()
		}
	case []byte:
		parentKey = p.([]byte)
	}
	if parentKey == nil || len(parentKey) == 0 {
		return false, errors.New("len(parentKey) is zero")
	}
	r.parentKey = parentKey
	return true, nil
}

// SetAttr sets the attr member.
func (r *Rename) SetAttr(b bool) {
	r.attr = b
}

// SetEncPass sets the pass member.
func (r *Rename) SetEncPass(p string) {
	r.pass = p
}

// SetExpire sets the expire member.
func (r *Rename) SetExpire(t int64) {
	r.expire = t
}

// Execute calls the C.k2hdkc_pm_rename_with_parent_wa function.
func (r *Rename) Execute(s *Session) (bool, error) {
	// old and new are must.
	if r.oldKey == nil || len(r.oldKey) == 0 || r.newKey == nil || len(r.newKey) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.key %v, r.val %v, r.result %v", r.newKey, r.oldKey, r.result)
	}
	cOldKey := C.CBytes(r.oldKey)
	defer C.free(cOldKey)
	cNewKey := C.CBytes(r.newKey)
	defer C.free(cNewKey)
	// parent(default is nil) is optional(null is acceptable for the k2hdkc C API).
	// For cgo, Go nil is equal to C NULL.
	cParentKey := C.CBytes(r.parentKey)
	defer C.free(cParentKey)
	// pass(default is nil) is optional(null is acceptable for the k2hdkc C API).
	// For cgo, Go nil is equal to C NULL.
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	r.result.ok = C.k2hdkc_pm_rename_with_parent_wa(
		s.handler,
		(*C.uchar)(cOldKey),
		C.size_t(len(r.oldKey)),
		(*C.uchar)(cNewKey),
		C.size_t(len(r.newKey)),
		(*C.uchar)(cParentKey),
		C.size_t(len(r.parentKey)),
		C._Bool(r.attr),
		cPass,
		expire)
	r.result.resCode = C.k2hdkc_get_res_code(s.handler)
	r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

	if r.result.ok == false {
		return false, errors.New("C.k2hdkc_pm_rename_with_parent_wa returned false")
	}
	return true, nil
}

// Result returns the pointer of RenameResult that has the result of Execute method.
func (r *Rename) Result() *RenameResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_rename_with_parent_wa has been successfully called.
func (r *RenameResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_rename_with_parent_wa in string format.
func (r *RenameResult) Error() string {
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
