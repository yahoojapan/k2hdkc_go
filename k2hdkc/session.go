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
	"C"
)
import (
	"errors"
	"fmt"
	"unsafe"
)

const defaultMaxSession = 1

// Session keeps configurations and is responsible for creating request handlers with a k2hdkc cluster and closing them.
type Session struct {
	handler C.k2hdkc_chmpx_h // uint64_t
	client  *Client
}

// String returns a text representation of the object.
func (s *Session) String() string {
	return fmt.Sprintf("[%v, %v]", s.handler, s.client)
}

// NewSession returns a new chmpx session with a k2hdkc cluster.
func NewSession(c *Client) (*Session, error) {
	if unSupportedOs {
		return nil, errors.New("k2hdkc currently works on linux only")
	}
	if unSupportedEndian {
		return nil, errors.New("k2hdkc_go currently works on little endian alignment only")
	}
	if isNotExistLibK2hdkc {
		return nil, errors.New("Please install the k2hdkc package at first")
	}
	if c == nil {
		return nil, errors.New("client is nil")
	}
	file := C.CString(c.file)
	cuk := C.CString(c.cuk)
	defer C.free(unsafe.Pointer(file))
	handler := C.k2hdkc_open_chmpx_full(file, C.short(c.port), cuk, C._Bool(c.rejoin), C._Bool(c.rejoinRetry), C._Bool(c.cleanup))
	if handler == C.K2HDKC_INVALID_HANDLE {
		return nil, fmt.Errorf("k2hdkc_open_chmpx_ex() = %v", handler)
	}
	return &Session{
		client:  c,
		handler: handler,
	}, nil
}

// Close closes a chmpx session with the k2hdkc cluster.
// NOTICE You must call Close() to avoid leaking file descriptor.
func (s *Session) Close() error {
	if s.client != nil {
		if result := C.k2hdkc_close_chmpx_ex(s.handler, C._Bool(s.client.cleanup)); !result {
			s.client.log.Warnf("C.k2hdkc_close_chmpx_ex() = %v", result)
			return fmt.Errorf("C.k2hdkc_close_chmpx_ex() = %v", result)
		}
		s.client.Close()
	}
	s.handler = C.K2HDKC_INVALID_HANDLE
	return nil
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
