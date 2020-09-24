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

// QueuePush holds arguments for C.k2hdkc_pm_q_push_wa and C.k2hdkc_pm_keyq_push_wa.
// It also holds a pointer of QueuePushResult.
type QueuePush struct {
	prefix []byte // must
	val    []byte // must
	key    []byte // option
	fifo   bool
	attr   bool
	pass   string
	expire int64
	result *QueuePushResult
}

// String returns a text representation of the object.
func (r *QueuePush) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v, %v, %v]", r.prefix, r.val, r.key, r.fifo, r.attr, r.pass, r.expire, r.result)
}

// QueuePushResult holds the result of QueuePush.Execute().
type QueuePushResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *QueuePushResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewQueuePush returns a new QueuePush which can push a value to a queue.
func NewQueuePush(p interface{}, v interface{}) (cmd *QueuePush, err error) {
	// prefix
	var prefix []byte
	switch p.(type) {
	default:
		return nil, fmt.Errorf("unsupported prefix data format %T", p)
	case string:
		if len(p.(string)) > 0 {
			var buf bytes.Buffer
			buf.WriteString(p.(string))
			buf.WriteRune('\u0000')
			prefix = buf.Bytes()
		}
	case []byte:
		prefix = p.([]byte)
	}
	if prefix == nil || len(prefix) == 0 {
		return nil, errors.New("len(prefix) is zero")
	}

	// val
	var val []byte
	switch v.(type) {
	default:
		return nil, fmt.Errorf("unsupported val data format %T", v)
	case string:
		if len(v.(string)) != 0 {
			var buf bytes.Buffer
			buf.WriteString(v.(string))
			buf.WriteRune('\u0000')
			val = buf.Bytes()
		}
	case []byte:
		val = v.([]byte)
	}
	if val == nil || len(val) == 0 {
		return nil, errors.New("len(val) is zero")
	}

	// set prefix & val
	r := &QueuePushResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &QueuePush{
		prefix: prefix,
		val:    val,
		key:    nil,
		fifo:   defaultOrderFIFO,
		attr:   defaultCheckAttr,
		pass:   "",
		expire: 0,
		result: r,
	}
	return c, nil
}

// NewQueuePushWithKey returns a new QueuePush which can use a key to push a value.
func NewQueuePushWithKey(p interface{}, v interface{}, k interface{}) (*QueuePush, error) {
	c, err := NewQueuePush(p, v)
	if c == nil || err != nil {
		return nil, fmt.Errorf("NewQueuePush(%q, %q) initialize error %v", p, v, err)
	}
	if ok, err := c.SetKey(k); err != nil {
		return nil, fmt.Errorf("QueuePush.SetKey(%q) = (%v, %v), want err == nil", k, ok, err)
	}
	return c, nil
}

// SetKey sets the key member.
func (r *QueuePush) SetKey(k interface{}) (bool, error) {
	// key
	var key []byte
	switch k.(type) {
	default:
		return false, fmt.Errorf("unsupported key data format %T", k)
	case string:
		if len(k.(string)) != 0 {
			key = []byte(k.(string) + "\u0000") // adds a null termination string
		}
	case []byte:
		key = k.([]byte)
	}
	if key == nil || len(key) == 0 {
		return false, errors.New("len(key) is zero")
	}
	r.key = key
	return true, nil
}

// UseFifo sets the fifo member.
func (r *QueuePush) UseFifo(b bool) {
	r.fifo = b
}

// SetAttr sets the attr member.
func (r *QueuePush) SetAttr(b bool) {
	r.attr = b
}

// SetEncPass sets the pass member.
func (r *QueuePush) SetEncPass(s string) {
	r.pass = s
}

// SetExpire sets the expire member.
func (r *QueuePush) SetExpire(t int64) {
	r.expire = t
}

// Execute calls the C.k2hdkc_pm_q_push_wa or C.k2hdkc_pm_keyq_push_wa function that push data to a queue.
func (r *QueuePush) Execute(s *Session) (bool, error) {
	// key(default is nil) is optional. Go nil is eqaul to C NULL.                                                                                            // The key argment of NULL is acceptable for the k2hdkc C API.
	if r.prefix == nil || len(r.prefix) == 0 || r.val == nil || len(r.val) == 0 || r.result == nil {
		return false, fmt.Errorf("some required members nil, r.prefix %v, r.val %v, r.result %v", r.prefix, r.val, r.result)
	}
	cPrefix := C.CBytes(r.prefix)
	defer C.free(cPrefix)
	cVal := C.CBytes(r.val)
	defer C.free(cVal)
	cKey := C.CBytes(r.key)
	defer C.free(cKey)
	// pass(default is nil) is optional. Go nil is eqaul to C NULL.                                                                                            // The pass argment of NULL is acceptable for the k2hdkc C API.
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	// WARNING: You can't set zero expire.
	if r.expire != 0 {
		expire = (*C.time_t)(&r.expire)
	}
	if len(r.key) == 0 {
		// bool k2hdkc_pm_q_push_wa(k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, const unsigned char* pval, size_t vallength, bool is_fifo, bool checkattr, const char* encpass, const time_t* expire)
		r.result.ok = C.k2hdkc_pm_q_push_wa(
			s.handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(r.prefix)),
			(*C.uchar)(cVal),
			C.size_t(len(r.val)),
			C._Bool(r.fifo),
			C._Bool(r.attr),
			cPass,
			expire)
		r.result.resCode = C.k2hdkc_get_res_code(s.handler)
		r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

		if r.result.ok == false {
			return false, errors.New("C.k2hdkc_pm_q_push_wa returned false")
		}
	} else {
		// bool k2hdkc_pm_keyq_push_wa(k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, const unsigned char* pkey, size_t keylength, const unsigned char* pval, size_t vallength, bool is_fifo, bool checkattr, const char* encpass, const time_t* expire)
		r.result.ok = C.k2hdkc_pm_keyq_push_wa(
			s.handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(r.prefix)),
			(*C.uchar)(cKey),
			C.size_t(len(r.key)),
			(*C.uchar)(cVal),
			C.size_t(len(r.val)),
			C._Bool(r.fifo),
			C._Bool(r.attr),
			cPass,
			expire)
		r.result.resCode = C.k2hdkc_get_res_code(s.handler)
		r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

		if r.result.ok == false {
			return false, errors.New("C.k2hdkc_pm_keyq_push_wa returned false")
		}
	}
	return true, nil
}

// Result returns the pointer of QueuePushResult that has the result of Execute method.
func (r *QueuePush) Result() *QueuePushResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_keyq_push_wa and C.k2hdkc_pm_q_push_wa has been successfully called.
func (r *QueuePushResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_keyq_push_wa and C.k2hdkc_pm_q_push_wa in string format.
func (r *QueuePushResult) Error() string {
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
