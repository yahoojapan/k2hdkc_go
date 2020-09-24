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

// QueueRemove holds arguments for C.k2hdkc_pm_q_remove_wa and C.k2hdkc_pm_keyq_remove_wa.
// It also holds a pointer of QueueRemoveResult.
type QueueRemove struct {
	prefix []byte
	pass   string
	count  int64
	fifo   bool
	useKq  bool
	result *QueueRemoveResult
}

// String returns a text representation of the object.
func (r *QueueRemove) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v]", r.prefix, r.pass, r.count, r.fifo, r.useKq, r.result)
}

// QueueRemoveResult holds the result of QueueRemove.Execute().
type QueueRemoveResult struct {
	ok         C._Bool
	resCode    C.dkcres_type_t // response
	subResCode C.dkcres_type_t // response(details)
}

// String returns a text representation of the object.
func (r *QueueRemoveResult) String() string {
	return fmt.Sprintf("[%v, %v, %v]", r.ok, r.resCode, r.subResCode)
}

// NewQueueRemove returns a new QueueRemove.
func NewQueueRemove(p interface{}, cnt int64) (*QueueRemove, error) {
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

	// set key & val
	r := &QueueRemoveResult{
		ok:         false,
		resCode:    0,
		subResCode: 0,
	}
	c := &QueueRemove{
		prefix: prefix,
		count:  cnt,
		fifo:   defaultOrderFIFO,
		pass:   "",
		useKq:  defaultUseKeyQueue, // false!
		result: r,
	}
	return c, nil
}

// NewQueueRemoveWithKeyQueue returns a new QueueRemove using key queue.
func NewQueueRemoveWithKeyQueue(p interface{}, cnt int64, kq bool) (cmd *QueueRemove, err error) {
	if c, error := NewQueueRemove(p, cnt); c != nil && error == nil {
		c.UseKeyQueue(kq)
		return c, nil
	}
	return nil, fmt.Errorf("NewQueueRemove(%q, %q) initialize error %v", p, cnt, err)
}

// UseKeyQueue sets the userKq member.
func (r *QueueRemove) UseKeyQueue(b bool) {
	r.useKq = b
}

// UseFifo sets the fifo member.
func (r *QueueRemove) UseFifo(b bool) {
	r.fifo = b
}

// SetEncPass sets the pass member.
func (r *QueueRemove) SetEncPass(s string) {
	r.pass = s
}

// Execute calls the C.k2hdkc_pm_q_remove_wp and C.k2hdkc_pm_keyq_remove_wp function that remove data in the queue.
func (r *QueueRemove) Execute(s *Session) (bool, error) {
	if r.prefix == nil || len(r.prefix) == 0 || r.result == nil {
		return false, fmt.Errorf("required members nil, r.prefix %v, r.result %v", r.prefix, r.result)
	}
	cPrefix := C.CBytes(r.prefix)
	defer C.free(cPrefix)
	cPass := C.CString(r.pass)
	defer C.free(unsafe.Pointer(cPass))
	if !r.useKq {
		// bool k2hdkc_pm_q_remove_wp(k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, int count, bool is_fifo, const char* encpass)
		r.result.ok = C.k2hdkc_pm_q_remove_wp(
			s.handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(r.prefix)),
			C.int(r.count),
			C._Bool(r.fifo),
			cPass)
		r.result.resCode = C.k2hdkc_get_res_code(s.handler)
		r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

		if r.result.ok == false {
			return false, errors.New("C.k2hdkc_pm_q_remove_wp returned false")
		}
	} else {
		// bool k2hdkc_pm_keyq_remove_wp(k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, int count, bool is_fifo, const char* encpass)
		r.result.ok = C.k2hdkc_pm_keyq_remove_wp(
			s.handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(r.prefix)),
			C.int(r.count),
			C._Bool(r.fifo),
			cPass)
		r.result.resCode = C.k2hdkc_get_res_code(s.handler)
		r.result.subResCode = C.k2hdkc_get_res_subcode(s.handler)

		if r.result.ok == false {
			return false, errors.New("C.C.k2hdkc_pm_keyq_remove_wp returned false")

		}
	}
	return true, nil
}

// Result returns the pointer of QueueRemoveResult that has the result of Execute method.
func (r *QueueRemove) Result() *QueueRemoveResult {
	if r.result != nil {
		return r.result
	}
	return nil
}

// Bool returns true if C.k2hdkc_pm_q_remove_wp and C.k2hdkc_pm_keyq_remove_wp has been successfully called.
func (r *QueueRemoveResult) Bool() bool {
	if !r.ok {
		return false
	}
	return true
}

// Error returns the errno of C.k2hdkc_pm_q_remove_wp and C.k2hdkc_pm_keyq_remove_wp in string format.
func (r *QueueRemoveResult) Error() string {
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
