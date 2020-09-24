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

/*
#cgo linux LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdio.h>
static int dlopen_k2hdkc() {
  dlerror();
  void* handler = dlopen("libk2hdkc.so.0", RTLD_LAZY);
  if (handler == NULL) {
    char* error = dlerror();
    if (error != NULL) {
      fprintf(stderr, "dlerror() %s\n", error);
      return -1;
    }
  }
  dlclose(handler);
  return 0;
}
*/
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"
)

// CasType defines cas value data type and length.
type CasType uint8

const (
	// CasType8 is the type def of unit8
	CasType8 CasType = 8
	// CasType16 is the type def of unit16
	CasType16 CasType = 16
	// CasType32 is the type def of unit32
	CasType32 CasType = 32
	// CasType64 is the type def of unit64
	CasType64 CasType = 64
)

const (
	defaultCuk             = ""
	defaultAutoRejoin      = true
	defaultAutoRejoinRetry = true
	defaultCleanup         = true
	defaultUseKeyQueue     = false
	defaultOrderFIFO       = true
	defaultCheckAttr       = true
)

var unSupportedOs = false
var unSupportedEndian = false
var isNotExistLibK2hdkc = false

// init checks if OS supports epoll system call and alignment of 64 bit data in memory is on little endian.
func init() {
	if runtime.GOOS != "linux" && runtime.GOARCH != "amd64" {
		fmt.Fprintf(os.Stderr, "k2hdkc currently works on linux only")
		unSupportedOs = true
	}
	i := uint32(1)
	b := (*[4]byte)(unsafe.Pointer(&i))
	if b[0] != 1 {
		fmt.Fprintf(os.Stderr, "k2hdkc_go currently works on little endian alignment only")
		unSupportedEndian = true
	}
	// rpm or deb install the k2hdkc.so in /usr/lib.
	if C.dlopen_k2hdkc() < 0 {
		fmt.Fprintf(os.Stderr, "Please install the k2hdkc package at first")
		isNotExistLibK2hdkc = true
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
