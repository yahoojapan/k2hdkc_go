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

// QueuePop holds arguments for C.k2hdkc_pm_q_pop_wa and C.k2hdkc_pm_keyq_pop_wa.
// It also holds a pointer of QueuePopResult.
type QueuePop struct {
	prefix []byte
	pass   string
	fifo   bool
	useKq  bool
	result *QueuePopResult
}

// String returns a text representation of the object.
func (r *QueuePop) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v]", r.prefix, r.pass, r.fifo, r.useKq, r.result)
}

// QueuePopResult holds the result of QueuePop.Execute().
type QueuePopResult struct {
	key        []byte
	val        []byte
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *QueuePopResult) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v]", r.key, r.val, r.ok, r.resCode, r.subResCode)
}

// NewQueuePop returns a QueuePop which can pop a value.
func NewQueuePop(p interface{}) (cmd *QueuePop, err error) {
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

	// set prefix
	r := &QueuePopResult{
		key:        nil,
		val:        nil,
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &QueuePop{
		prefix: prefix,
		fifo:   defaultOrderFIFO,
		pass:   "",
		useKq:  defaultUseKeyQueue, // false
		result: r,
	}
	return c, nil
}

// NewQueuePopWithKeyQueue returns a QueuePop which can specify a key to pop a value.
func NewQueuePopWithKeyQueue(p interface{}, kq bool) (cmd *QueuePop, err error) {
	if c, error := NewQueuePop(p); c != nil && error == nil {
		c.UseKeyQueue(kq)
		return c, nil
	}
	return nil, fmt.Errorf("NewQueuePop(%q) initialize error %v", p, err)
}

// UseKeyQueue sets the useKq member.
func (r *QueuePop) UseKeyQueue(b bool) {
	r.useKq = b
}

// UseFifo sets the filo member.
func (r *QueuePop) UseFifo(b bool) {
	r.fifo = b
}

// SetEncPass sets the pass member.
func (r *QueuePop) SetEncPass(s string) {
	r.pass = s
}

// Execute calls the C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp function that pops from the k2hdkc cluster.
func (r *QueuePop) Execute(s *Session) (bool, error) {
	if r.prefix == nil || len(r.prefix) == 0 || r.result == nil {
		return false, fmt.Errorf("required members nil, r.prefix %v, r.result %v", r.prefix, r.result)
	}
	cPrefix := C.CBytes(r.prefix)
	defer C.free(cPrefix)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	var cRetKey (*C.uchar)
	var cRetKeyLen C.size_t
	var cRetVal (*C.uchar)
	var cRetValLen C.size_t
	if r.useKq {
		// bool k2hdkc_pm_keyq_pop_wp(
		//   k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, bool is_fifo, const char* encpass,
		//   unsigned char** ppkey, size_t* pkeylength, unsigned char** ppval, size_t* pvallength);
		r.result.ok = C.k2hdkc_pm_keyq_pop_wp(
			s.handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(r.prefix)),
			C._Bool(r.fifo),
			cPass,
			&cRetKey,
			&cRetKeyLen,
			&cRetVal,
			&cRetValLen)
		r.result.resCode = C.k2hdkc_get_res_code(s.handler)
		r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
		defer C.free(unsafe.Pointer(cRetKey))
		defer C.free(unsafe.Pointer(cRetVal))
		if !r.result.ok {
			return false, errors.New("C.k2hdkc_pm_keyq_pop_wp returned false")
		}
		r.result.key = C.GoBytes(unsafe.Pointer(cRetKey), C.int(cRetKeyLen))
		r.result.val = C.GoBytes(unsafe.Pointer(cRetVal), C.int(cRetValLen))

	} else {
		// bool k2hdkc_pm_q_pop_wp(
		//   k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, bool is_fifo, const char* encpass,
		//   unsigned char** ppval, size_t* pvallength);
		r.result.ok = C.k2hdkc_pm_q_pop_wp(
			s.handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(r.prefix)),
			C._Bool(r.fifo),
			cPass,
			&cRetVal,
			&cRetValLen)
		r.result.resCode = C.k2hdkc_get_res_code(s.handler)
		r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)
		defer C.free(unsafe.Pointer(cRetVal))
		if !r.result.ok {
			return false, errors.New("C.k2hdkc_pm_q_pop_wp returned false")
		}
		r.result.val = C.GoBytes(unsafe.Pointer(cRetVal), C.int(cRetValLen))
	}
	return true, nil
}

// Result returns the pointer of QueuePopResult that has the result of Execute method.
func (r *QueuePop) Result() *QueuePopResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp has been successfully called.
func (r *QueuePopResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp in string format.
func (r *QueuePopResult) Error() string {
	return fmt.Sprintf("%v %v",
		C.GoString(C.str_dkcres_result_type(r.resCode)),
		C.GoString(C.str_dkcres_subcode_type(r.subResCode)))
}

// KeyBytes returns the key data in C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp response in binary format.
func (r *QueuePopResult) KeyBytes() []byte {
	return r.key
}

// KeyString returns the key data in C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp response in text format.
func (r *QueuePopResult) KeyString() string {
	keyLen := len(r.key)
	if keyLen > 0 {
		return string(r.key[:keyLen-1]) // make a new string without the null termination
	}
	return ""
}

// ValBytes returns the value data in C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp response in string format.
func (r *QueuePopResult) ValBytes() []byte {
	return r.val
}

// ValString returns the value data in C.k2hdkc_pm_q_pop_wp and C.k2hdkc_pm_keyq_pop_wp response in text format.
func (r *QueuePopResult) ValString() string {
	valLen := len(r.val)
	if valLen > 0 {
		return string(r.val[:valLen-1]) // make a new string without the null termination
	}
	return ""
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
