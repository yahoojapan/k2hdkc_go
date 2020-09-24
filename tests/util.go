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

package k2hdkctest

import (
	// #cgo CFLAGS: -g -Wall -O2 -Wall -Wextra -Wno-unused-variable -Wno-unused-parameter -I. -I/usr/include/k2hdkc
	// #cgo LDFLAGS: -L/usr/lib -lk2hdkc
	// #include <stdlib.h>
	// #include "k2hdkc.h"
	"C"
)

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
	"unsafe"
)

var stopCluster = false

// Variables declared in byte arrayrs can hold binary data or text data.
type testCasSetData struct {
	k []byte
	o string
	n string
	w string // password
	e int64  // duration of expiration(unit seconds)
}

type kv struct {
	d string // description
	k []byte // key
	v []byte // val
	r bool   // remove subkeys if true.
	s bool   // true if use string format when saving the data.
	p string // password
	e int64  // expire
}

type testRenameData struct {
	o []byte // oldkey
	n []byte // newkey
	p []byte // parentkey
	w string // password
	e int64  // duration of expiration(unit seconds)
	c bool   // copy attr if true
	s bool   // string or binary
}

type testCasGetData struct {
	k  []byte
	o  []byte
	n  []byte
	w  string // password
	vl int8   // value length
}

type testCasIncDecData struct {
	k []byte
	w string // password
	e int64  // duration of expiration(unit seconds)
	i bool   // increment or decrement
}

type testCasInitData struct {
	k []byte
	v string
	w string // password
	e int64  // duration of expiration(unit seconds)
}

type testQData struct {
	p []byte // prefix
	v []byte // val
	k []byte // key
	f bool   // fifo
	c bool   // copy attr if true
	s bool   // string or binary
	w string // password
	e int64  // duration of expiration(unit seconds)
}

type testQRemoveData struct {
	p []byte // prefix
	f bool   // fifo
	u bool   // use key queue
	c int64  // count
	w string // password
	s bool   // true if use string format when saving the data.
}

type testSubKeyData struct {
	d    string
	key  kv   // key(parent)
	skey kv   // subkey
	s    bool // true if use string format when saving the data.
}

type testSubKeysData struct {
	d    string // description
	key  kv     // parentkey
	keys []kv   // subkeys
	s    bool   // true if use string format when saving the data.
}

// util's functions use k2hdkc C APIs to add test data and investigate the test result in this test.

// TODO binary only
func callPopQueue(prefix []byte, val []byte, key []byte, pass string) ([]byte, []byte, error) {
	// key and pass are optional data.
	if len(prefix) == 0 || len(val) == 0 {
		return nil, nil, fmt.Errorf("len(%v) == %v || len(%q) == %v, want > 0", prefix, len(prefix), val, len(val))
	}
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return nil, nil, fmt.Errorf("C.k2hdkc_open_chmpx_ex() = %v, want != C.K2HDKC_INVALID_HANDLE", handler)
	}
	cPrefix := C.CBytes(prefix)
	defer C.free(cPrefix)
	fifo := true
	cPass := C.CString(pass)
	defer C.free(unsafe.Pointer(cPass))
	var cRetKey (*C.uchar)
	var cRetKeyLen C.size_t
	var cRetVal (*C.uchar)
	var cRetValLen C.size_t
	var rval []byte
	var rkey []byte
	if len(key) != 0 {
		// bool k2hdkc_pm_keyq_pop_wp(
		//   k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, bool is_fifo, const char* encpass,
		//   unsigned char** ppkey, size_t* pkeylength, unsigned char** ppval, size_t* pvallength);
		ok := C.k2hdkc_pm_keyq_pop_wp(
			handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(prefix)),
			C._Bool(fifo),
			cPass,
			&cRetKey,
			&cRetKeyLen,
			&cRetVal,
			&cRetValLen)
		//resCode := C.k2hdkc_get_res_code(handler)
		//subResCode := C.k2hdkc_get_res_subcode(handler)
		//fmt.Printf("%v %v\n", C.GoString(C.str_dkcres_result_type(resCode)), C.GoString(C.str_dkcres_subcode_type(subResCode)))
		if cRetKey != nil {
			defer C.free(unsafe.Pointer(cRetKey))
		}
		if cRetVal != nil {
			defer C.free(unsafe.Pointer(cRetVal))
		}
		if !ok {
			return nil, nil, errors.New("C.k2hdkc_pm_keyq_pop_wp returned false")
		}
		rkey = C.GoBytes(unsafe.Pointer(cRetKey), C.int(cRetKeyLen))
		rval = C.GoBytes(unsafe.Pointer(cRetVal), C.int(cRetValLen))
	} else {
		// bool k2hdkc_pm_q_pop_wp(
		//   k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, bool is_fifo, const char* encpass,
		//   unsigned char** ppval, size_t* pvallength);
		ok := C.k2hdkc_pm_q_pop_wp(
			handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(prefix)),
			C._Bool(fifo),
			cPass,
			&cRetVal,
			&cRetValLen)
		//resCode := C.k2hdkc_get_res_code(handler)
		//subResCode := C.k2hdkc_get_res_subcode(handler)
		//fmt.Printf("%v %v\n", C.GoString(C.str_dkcres_result_type(resCode)), C.GoString(C.str_dkcres_subcode_type(subResCode)))
		if cRetVal != nil {
			defer C.free(unsafe.Pointer(cRetVal))
		}
		if !ok {
			return nil, nil, errors.New("C.k2hdkc_pm_q_pop_wp returned false")
		}
		rval = C.GoBytes(unsafe.Pointer(cRetVal), C.int(cRetValLen))
	}
	C.k2hdkc_close_chmpx_ex(handler, true)

	//fmt.Printf("callSetSubkeys(%q, %q) pack %v packLen %v\n", pk, sk, pack, packLen)
	return rval, rkey, nil
}

func callCasSet(key []byte, old []byte, new []byte, pass string) (bool, error) {
	// key and pass are optional data.
	if len(key) == 0 || len(old) == 0 || len(new) == 0 {
		return false, fmt.Errorf("len(%v) == %v || len(%q) == %v || len(%q) == %v, want > 0", key, len(key), old, len(old), new, len(new))
	}
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return false, fmt.Errorf("C.k2hdkc_open_chmpx_ex() = %v, want != C.K2HDKC_INVALID_HANDLE", handler)
	}

	cKey := C.CBytes(key)
	defer C.free(cKey)
	cPass := C.CString(pass)
	defer C.free(unsafe.Pointer(cPass))
	var cExpire *C.time_t
	// length of old and new must be same.
	if len(old) != len(new) {
		return false, fmt.Errorf("len(old) %v len(new) %v must be same", len(old), len(new))
	}

	switch len(old) {
	default:
		return false, fmt.Errorf("unsupported data format %T", old)
	case 1:
		{
			// bool k2hdkc_pm_cas8_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint8_t oldval, uint8_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint8_t)((uint8)(old[0]))
			cNew := (C.uint8_t)((uint8)(new[0]))
			ok := C.k2hdkc_pm_cas8_set_wa(
				handler,
				(*C.uchar)(cKey),
				C.size_t(len(key)),
				cOld,
				cNew,
				cPass,
				cExpire)

			if !ok {
				return false, errors.New("C.k2hdkc_pm_cas8_set_wa returned false")
			}
			// fmt.Printf("C.k2hdkc_pm_cas8_set_wa(%v, %v, %v) returned true", key, old, new)
		}
	case 2:
		{
			// bool k2hdkc_pm_cas16_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint16_t oldval, uint16_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint16_t)(
				((uint16)(old[0])) | // first
					((uint16)(old[1]) << 8))
			cNew := (C.uint16_t)(
				((uint16)(new[0])) | // first
					((uint16)(new[1]) << 8))
			ok := C.k2hdkc_pm_cas16_set_wa(
				handler,
				(*C.uchar)(cKey),
				C.size_t(len(key)),
				cOld,
				cNew,
				cPass,
				cExpire)
			if !ok {
				return false, errors.New("C.k2hdkc_pm_cas16_set_wa returned false")
			}
			// fmt.Printf("C.k2hdkc_pm_cas16_set_wa(%v, %v, %v) returned true.", key, old, new)
		}
	case 4:
		{
			// bool k2hdkc_pm_cas32_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint32_t oldval, uint32_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint32_t)(
				((uint32)(old[0])) | // first
					((uint32)(old[1]) << 8) | // second
					((uint32)(old[2]) << 16) | // 3rd
					((uint32)(old[3]) << 24)) // 4th
			cNew := (C.uint32_t)(
				((uint32)(new[0])) | // first
					((uint32)(new[1]) << 8) | // second
					((uint32)(new[2]) << 16) | // 3rd
					((uint32)(new[3]) << 24)) // 4th
			ok := C.k2hdkc_pm_cas32_set_wa(
				handler,
				(*C.uchar)(cKey),
				C.size_t(len(key)),
				cOld,
				cNew,
				cPass,
				cExpire)
			if !ok {
				return false, errors.New("C.k2hdkc_pm_cas32_set_wa returned false")
			}
			// fmt.Printf("C.k2hdkc_pm_cas32_set_wa(%v, %v, %v) returned true", key, old, new)
		}
	case 8:
		{
			// bool k2hdkc_pm_cas64_set_wa(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength, uint64_t oldval, uint64_t newval, const char* encpass, const time_t* expire)
			cOld := (C.uint64_t)(
				((uint64)(old[0])) | // 1st
					((uint64)(old[1]) << 8) | // 2nd
					((uint64)(old[2]) << 16) | // 3rd
					((uint64)(old[3]) << 24) | // 4th
					((uint64)(old[4]) << 32) | // 5th
					((uint64)(old[5]) << 40) | // 6th
					((uint64)(old[6]) << 48) | // 7th
					((uint64)(old[7]) << 56)) // 8th
			cNew := (C.uint64_t)(
				((uint64)(new[0])) | // 1st
					((uint64)(new[1]) << 8) | // 2nd
					((uint64)(new[2]) << 16) | // 3rd
					((uint64)(new[3]) << 24) | // 4th
					((uint64)(new[4]) << 32) | // 5th
					((uint64)(new[5]) << 40) | // 6th
					((uint64)(new[6]) << 48) | // 7th
					((uint64)(new[7]) << 56)) // 8th
			ok := C.k2hdkc_pm_cas64_set_wa(
				handler,
				(*C.uchar)(cKey),
				C.size_t(len(key)),
				cOld,
				cNew,
				cPass,
				cExpire)
			if !ok {
				return false, errors.New("C.k2hdkc_pm_cas64_set_wa returned false")
			}
			// fmt.Printf("C.k2hdkc_pm_cas64_set_wa(%v, %v, %v) returned true", key, old, new)
		}
	}
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		return false, fmt.Errorf("C.k2hdkc_close_chmpx_ex() = %v, want true", cresult)
	}
	//fmt.Printf("callSetSubkeys(%q, %q) pack %v packLen %v\n", pk, sk, pack, packLen)
	return true, nil
}

func callPushQueue(p interface{}, v interface{}, k interface{}, pass string) (bool, error) {
	var prefix []byte
	var key []byte
	var val []byte

	switch p.(type) {
	default:
		return false, fmt.Errorf("unsupported key data format %T", p)
	case string:
		if len(p.(string)) != 0 {
			prefix = []byte(p.(string) + "\u0000") // adds a null termination string
		}
	case []byte:
		prefix = p.([]byte)
	}
	if prefix == nil || len(prefix) == 0 {
		return false, errors.New("len(prefix) is zero")
	}
	switch v.(type) {
	default:
		return false, fmt.Errorf("unsupported key data format %T", v)
	case string:
		if len(v.(string)) != 0 {
			val = []byte(v.(string) + "\u0000") // adds a null termination string
		}
	case []byte:
		val = v.([]byte)
	}
	if val == nil || len(val) == 0 {
		return false, errors.New("len(val) is zero")
	}
	// key and pass are optional data. otherwise, they are must.
	if len(prefix) == 0 || len(val) == 0 {
		return false, fmt.Errorf("len(%v) == %v || len(%q) == %v, want > 0", prefix, len(prefix), val, len(val))
	}
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return false, fmt.Errorf("C.k2hdkc_open_chmpx_ex() = %v, want != C.K2HDKC_INVALID_HANDLE", handler)
	}

	cPrefix := C.CBytes(prefix)
	defer C.free(cPrefix)
	cVal := C.CBytes(val)
	defer C.free(cVal)
	cKey := C.CBytes(key)
	defer C.free(cKey)
	cPass := C.CString(pass)
	defer C.free(unsafe.Pointer(cPass))
	var expire *C.time_t
	fifo := true
	attr := true
	var ok C._Bool
	if len(key) == 0 {
		// bool k2hdkc_pm_q_push_wa(k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, const unsigned char* pval, size_t vallength, bool is_fifo, bool checkattr, const char* encpass, const time_t* expire)
		ok = C.k2hdkc_pm_q_push_wa(
			handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(prefix)),
			(*C.uchar)(cVal),
			C.size_t(len(val)),
			C._Bool(fifo),
			C._Bool(attr),
			cPass,
			expire)
		if ok == false {
			return false, errors.New("C.k2hdkc_pm_q_push_wa returned false")
		}
	} else {
		// bool k2hdkc_pm_keyq_push_wa(k2hdkc_chmpx_h handle, const unsigned char* pprefix, size_t prefixlength, const unsigned char* pkey, size_t keylength, const unsigned char* pval, size_t vallength, bool is_fifo, bool checkattr, const char* encpass, const time_t* expire)
		ok = C.k2hdkc_pm_keyq_push_wa(
			handler,
			(*C.uchar)(cPrefix),
			C.size_t(len(prefix)),
			(*C.uchar)(cKey),
			C.size_t(len(key)),
			(*C.uchar)(cVal),
			C.size_t(len(val)),
			C._Bool(fifo),
			C._Bool(attr),
			cPass,
			expire)
		if ok == false {
			return false, errors.New("C.k2hdkc_pm_keyq_push_wa returned false")
		}
	}
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		return false, fmt.Errorf("C.k2hdkc_close_chmpx_ex() = %v, want true", cresult)
	}
	//fmt.Printf("callSetSubkeys(%q, %q) pack %v packLen %v\n", pk, sk, pack, packLen)
	return true, nil
}

func callSetSubkeys(k interface{}, sk interface{}) (bool, error) {
	var key []byte
	var skeys [][]byte

	// key
	switch k.(type) {
	default:
		return false, fmt.Errorf("unsupported key data format %T", k)
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
		return false, errors.New("len(key) is zero")
	}

	// subkeys
	switch sk.(type) {
	default:
		return false, fmt.Errorf("unsupported key data format %T", k)
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
		return false, errors.New("len(skeys) is zero")
	}

	// fmt.Printf("callSetSubkeys(%q, %q) key %v skeys %v\n", k, sk, key, skeys)

	// make keypack
	cKey := C.CBytes(key)
	defer C.free(unsafe.Pointer(cKey))

	pack := make([]C.PK2HDKCKEYPCK, len(skeys))
	for i, skey := range skeys {
		key := (*C.uchar)(C.CBytes(skey)) // key will be freed after calling bC.k2hdkc_pm_set_subkeys
		length := C.size_t(len(skey))
		pack[i] = &C.K2HKEYPCK{length: length, pkey: key}
	}

	// connect
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return false, fmt.Errorf("C.k2hdkc_open_chmpx_ex() = %v, want != C.K2HDKC_INVALID_HANDLE", handler)
	}

	// send request
	result := C.k2hdkc_pm_set_subkeys(
		handler,
		(*C.uchar)(cKey),
		(C.size_t)(len(key)),
		pack[0],
		(C.int)(len(pack)))

	// frees keys in keypack
	for i := range skeys {
		key := pack[i].pkey
		defer C.free(unsafe.Pointer(key))
	}

	if result == false {
		return false, fmt.Errorf("C.k2hdkc_pm_get_subkeys() = %v, want true", result)
	}
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		return false, fmt.Errorf("C.k2hdkc_close_chmpx_ex() = %v, want true", cresult)
	}
	return true, nil
}

func callGetSubkeys(k interface{}) (int, [][]byte, error) {
	var key []byte

	// key
	switch k.(type) {
	default:
		return -1, nil, fmt.Errorf("unsupported key data format %T", k)
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
		return -1, nil, errors.New("len(key) is zero")
	}

	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return -1, nil, fmt.Errorf("C.k2hdkc_open_chmpx_ex() = %v, want != C.K2HDKC_INVALID_HANDLE", handler)
	}

	cKey := C.CBytes(key)
	defer C.free(cKey)
	var keypack C.PK2HDKCKEYPCK
	var keypackLen C.int
	result := C.k2hdkc_pm_get_subkeys(
		handler,
		(*C.uchar)(cKey),
		C.size_t(len(key)),
		&keypack,
		&keypackLen,
	)
	if result == false {
		return -1, nil, fmt.Errorf("C.k2hdkc_pm_get_subkeys() = %v, want true", result)
	}
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		return -1, nil, fmt.Errorf("C.k2hdkc_close_chmpx_ex() = %v, want true", cresult)
	}
	if keypackLen == 0 {
		return 0, nil, nil
	}

	var theCArray C.PK2HDKCKEYPCK = keypack
	length := (int)(keypackLen)
	cslice := (*[1 << 30]C.K2HKEYPCK)(unsafe.Pointer(theCArray))[:length:length]
	slice := make([][]byte, length)
	for i, data := range cslice {
		sk := C.GoBytes(unsafe.Pointer(data.pkey), (C.int)(data.length))
		// fmt.Printf("i %v data %T pkey %v length %v sk %v\n", i, data, data.pkey, data.length, sk)
		slice[i] = sk
	}
	return (int)(keypackLen), slice, nil
}

func callGetSubkeysString(k interface{}) (int, []string, error) {
	var key []byte

	// key
	switch k.(type) {
	default:
		return -1, nil, fmt.Errorf("unsupported key data format %T", k)
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
		return -1, nil, errors.New("len(key) is zero")
	}

	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return -1, nil, fmt.Errorf("C.k2hdkc_open_chmpx_ex() = %v, want != C.K2HDKC_INVALID_HANDLE", handler)
	}

	cKey := C.CBytes(key)
	defer C.free(cKey)
	var keypack C.PK2HDKCKEYPCK
	var keypackLen C.int
	result := C.k2hdkc_pm_get_subkeys(
		handler,
		(*C.uchar)(cKey),
		C.size_t(len(key)),
		&keypack,
		&keypackLen,
	)
	if result == false {
		return -1, nil, fmt.Errorf("C.k2hdkc_pm_get_subkeys() = %v, want true", result)
	}
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		return -1, nil, fmt.Errorf("C.k2hdkc_close_chmpx_ex() = %v, want true", cresult)
	}
	if keypackLen == 0 {
		return 0, nil, nil
	}

	var theCArray C.PK2HDKCKEYPCK = keypack
	length := (int)(keypackLen)
	cslice := (*[1 << 30]C.K2HKEYPCK)(unsafe.Pointer(theCArray))[:length:length]
	slice := make([]string, length)
	for i, data := range cslice {
		sk := C.GoBytes(unsafe.Pointer(data.pkey), (C.int)(data.length))
		//fmt.Printf("i %v data %T pkey %v length %v sk %v\n", i, data, data.pkey, data.length, sk)
		valLen := len(sk)
		if valLen > 0 {
			slice[i] = string(sk[:valLen-1]) // make a new string without the null termination
		} else {
			slice[i] = ""
		}
	}
	return (int)(keypackLen), slice, nil
}

func getKey(d kv) (bool, []byte, error) {
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return false, nil, fmt.Errorf("C.k2hdkc_open_chmpx_ex() == %v, should be a valid fd", handler)
	}

	cKey := C.CBytes(d.k) // func C.CBytes([]byte) unsafe.Pointer
	keyLen := C.size_t(len(d.k))
	defer C.free(cKey)
	cPass := C.CString(d.p) // func C.CString(string) *C.char
	defer C.free(unsafe.Pointer(cPass))

	var cRetValue *C.uchar // value:(*main._Ctype_char)(nil) type:*main._Ctype_char
	var valLen C.size_t    // valLen value:0x0 type:main._Ctype_size_t
	var result C._Bool = C.k2hdkc_pm_get_value_wp(
		handler,
		(*C.uchar)(cKey),
		keyLen,
		cPass,
		&cRetValue,
		&valLen)
	defer C.free(unsafe.Pointer(cRetValue))
	val := C.GoBytes(unsafe.Pointer(cRetValue), C.int(valLen))
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		fmt.Fprintf(os.Stderr, "C.k2hdkc_close_chmpx_ex returned false.")
	}
	if result == false {
		return false, nil, errors.New("C.k2hdkc_pm_get_value_wp returned false")
	}
	return true, val, nil
}

func getKeyString(d kv) (bool, string, error) {
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return false, "", fmt.Errorf("C.k2hdkc_open_chmpx_ex() == %v, should be a valid fd", handler)
	}
	var key []byte
	if len(d.k) != 0 {
		key = []byte(string(d.k) + "\u0000") // adds a null termination string
	}
	cKey := C.CBytes(key) // func C.CBytes([]byte) unsafe.Pointer
	keyLen := C.size_t(len(key))
	defer C.free(cKey)
	cPass := C.CString(d.p) // func C.CString(string) *C.char
	defer C.free(unsafe.Pointer(cPass))

	var cRetValue *C.uchar // value:(*main._Ctype_char)(nil) type:*main._Ctype_char
	var valLen C.size_t    // valLen value:0x0 type:main._Ctype_size_t
	var result C._Bool = C.k2hdkc_pm_get_value_wp(
		handler,
		(*C.uchar)(cKey),
		keyLen,
		cPass,
		&cRetValue,
		&valLen)
	defer C.free(unsafe.Pointer(cRetValue))
	var val []byte
	val = C.GoBytes(unsafe.Pointer(cRetValue), C.int(valLen))
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		fmt.Fprintf(os.Stderr, "C.k2hdkc_close_chmpx_ex returned false.")
	}
	if result == false {
		return false, "", errors.New("C.k2hdkc_pm_get_value_wp returned false")
	}
	valLen2 := len(val)
	if valLen2 > 0 {
		return true, string(val[:valLen2-1]), nil // make a new string without the null termination
	}
	return false, "", errors.New("C.k2hdkc_pm_get_value_wp returned empty string")
}

func callGetKey(d kv) (bool, error) {
	if len(d.k) == 0 {
		return false, fmt.Errorf(`len(%q) == 0, should be >0`, d.k)
	}
	var ok bool
	var err error
	if !d.s {
		var val []byte
		if ok, val, err = getKey(d); !ok {
			return ok, err
		}
		if !bytes.Equal(val, d.v) {
			return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp(%q) = %q, want %q", d.k, val, d.v)
		}
	} else {
		var val string
		if ok, val, err = getKeyString(d); !ok {
			return ok, err
		}
		if val != string(d.v) {
			return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp(%q) = %q, want %q", string(d.k), val, string(d.v))
		}
	}
	if d.p != "" {
		// Check if I can't get data invalid password.
		r := rand.New(rand.NewSource(time.Now().UnixNano() + (int64)(os.Getpid())))
		rpass := strconv.FormatInt(r.Int63(), 10)
		d2 := kv{d: d.d, k: d.k, v: d.v, p: rpass, e: d.e, r: d.r, s: d.s}
		if !d.s {
			if ok, val, err := getKey(d2); len(val) != 0 {
				return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp(k=%q, pass=%q) = (%v, %q, %q), want len(val) == 0", d2.k, rpass, ok, err, val)
			}
		} else {
			if ok, val, err := getKeyString(d2); len(val) != 0 {
				return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp(k=%q, pass=%q) = (%v, %q, %q), want len(val) == 0", string(d2.k), rpass, ok, err, val)
			}
		}
	}

	if d.e != 0 {
		// Check if I can't get expired data.
		time.Sleep(time.Duration(d.e+1) * time.Second)
		if !d.s {
			if ok, val, err := getKey(d); len(val) != 0 {
				return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp(k=%q) = (%v, %q, %q), want len(val) == 0(expired!)", d.k, ok, err, val)
			}
		} else {
			if ok, val, err := getKeyString(d); len(val) != 0 {
				return false, fmt.Errorf("C.k2hdkc_pm_get_value_wp(k=%q) = (%v, %q, %q), want len(val) == 0(expired!)", string(d.k), ok, err, val)
			}
		}
	}
	return true, nil
}

func clearIfExists(k interface{}) (bool, error) {
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
	if key != nil && len(key) != 0 {
		file := "../cluster/slave.yaml"
		cFile := C.CString(file)
		defer C.free(unsafe.Pointer(cFile))

		handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
		if handler == C.K2HDKC_INVALID_HANDLE {
			return false, fmt.Errorf("C.k2hdkc_open_chmpx_ex returned invalid handler %v", handler)
		}

		cKey := C.CBytes(key)
		defer C.free(cKey)
		/*
			bool k2hdkc_pm_remove_all(k2hdkc_chmpx_h handle, const unsigned char* pkey, size_t keylength)
		*/
		result := C.k2hdkc_pm_remove_all(
			handler,
			(*C.uchar)(cKey),
			C.size_t(len(key)))

		if result == false {
			if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
				return false, errors.New("C.k2hdkc_close_chmpx_ex returned false")
			}
			return false, errors.New("C.C.k2hdkc_pm_remove_all returned NULL")
		}
		if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
			return false, errors.New("C.k2hdkc_pm_remove_all returned true, but C.k2hdkc_close_chmpx_ex returned false")
		}
	}
	return true, nil
	//return false, fmt.Errorf("len(%v) %v should be over zero.", k, len(k))
}

//
// saveData is a function to save data in the k2hdkc cluster.
// It returns false if key is empty because no empty key saves in k2hdkc cluster.
// Data expires in 60 seconds because test should be done in short duration.
// Expiring random keys are so simple that we need not only remember them for deletion
// and also need not delete them.
//
func saveData(k interface{}, v interface{}, p string) (bool, error) {
	var key []byte
	var val []byte

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
	switch v.(type) {
	default:
		return false, fmt.Errorf("unsupported key data format %T", k)
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
		return false, errors.New("len(key) is zero")
	}
	file := "../cluster/slave.yaml"
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	handler := C.k2hdkc_open_chmpx_ex(cFile, 8031, true, true, true)
	if handler == C.K2HDKC_INVALID_HANDLE {
		return false, fmt.Errorf("C.k2hdkc_open_chmpx_ex returned invalid handler %v", handler)
	}

	cKey := C.CBytes(key)
	defer C.free(cKey)
	cVal := C.CBytes(val)
	defer C.free(cVal)
	cPass := C.CString(p)
	defer C.free(unsafe.Pointer(cPass))
	expire := (C.time_t)(60) // expire in 60sec.
	result := C.k2hdkc_pm_set_value_wa(
		handler,
		(*C.uchar)(cKey),
		C.size_t(len(key)),
		(*C.uchar)(cVal),
		C.size_t(len(val)),
		C._Bool(false),
		cPass,
		&expire)
	if result == false {
		if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
			return false, errors.New("C.k2hdkc_close_chmpx_ex returned false")
		}
		return false, errors.New("C.k2hdkc_pm_set_value_wa returned NULL")
	}
	if cresult := C.k2hdkc_close_chmpx_ex(handler, true); !cresult {
		return false, errors.New("C.k2hdkc_pm_set_value_wa returned true, but C.k2hdkc_close_chmpx_ex returned false")
	}
	return true, nil
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
