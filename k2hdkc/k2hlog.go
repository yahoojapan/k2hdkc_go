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
	// #cgo CFLAGS: -g -O2 -Wall -Wextra -Wno-unused-variable -Wno-unused-parameter -I. -I/usr/include/k2hdkc -I/usr/include/chmpx -I/usr/include/k2hash
	// #cgo LDFLAGS: -L/usr/lib -lk2hdkc -lchmpx -lk2hash
	// #include <stdlib.h>
	// #include "k2hdkc.h"
	// #include "chmpx.h"
	// #include "k2hash.h"
	"C"
	"fmt"
	"io"
	"log"
	"os"
)
import (
	"sync"
	"unsafe"
)

type logSeverity uint8

// internal
var k2hlog *K2hLog

// sync.Once holds a uint32 variable whick will set 1 after a function value passed by Once.Do().
// In Do(), Mutex.Lock() calls before the function calls and Mutex.Unlock() calls when enclosing Do().
var once sync.Once

// SeveritySilent means this library logs nothing.
// SeverityError means this library logs only error message.
// SeverityWarning means this library logs warning and error message.
// SeverityInfo means this library logs normal, warning and error message.
// SeverityDump means this library logs all message.
const (
	SeveritySilent logSeverity = 1 << iota
	SeverityError
	SeverityWarning
	SeverityInfo
	SeverityDump
)

// LibSeveritySilent means dependent libs logs nothing.
// LibComlog means dependent libraries logs only communication message.
// LibK2hdkc means k2hdkc library logs in the current severity level.
// LibChmpx means chmpx and k2hdkc libraries logs only communication message.
// Libk2hash means k2hash, chmpx and k2hdkc libraries logs only communication message.
const (
	LibSeveritySilent logSeverity = 1 << iota
	LibComlog
	LibK2hdkc
	LibChmpx
	LibK2hash
)

var logSeverityText = map[logSeverity]string{
	SeveritySilent:  "SILENT",
	SeverityError:   "ERROR",
	SeverityWarning: "WARN",
	SeverityInfo:    "INFO",
	SeverityDump:    "DUMP",
}

const (
	defaultLogFile        = ""
	defaultLogSeverity    = SeverityError
	defaultLibLogSeverity = LibSeveritySilent
	defaultBundleLibLog   = false
)

// K2hLog holds pointer to log.Logger and logging configurations.
type K2hLog struct {
	file         string
	fp           *os.File
	severity     logSeverity
	libSeverity  logSeverity
	bundleLibLog bool
	logger       *log.Logger
}

// K2hLogInstance initializes a K2hLog structure
func K2hLogInstance() *K2hLog {
	// In once.Do method, Mutex.Lock() calls before the anonymous function calls.
	// defer Mutex.Unlock() calls at the end of the enclosing function.
	once.Do(func() {
		if k2hlog == nil {
			k2hlog = newK2hLog()
		}
	})
	return k2hlog
}

// newK2hLog initializes a K2hLog structure.
func newK2hLog() *K2hLog {
	severity := defaultLogSeverity
	if p := os.Getenv("GO_K2HDKC_DBGLEVEL"); p != "" {
		if p2 := normalizeSeverity(p); p2 != severity {
			severity = p2
		}
	}
	file := defaultLogFile
	if f := os.Getenv("GO_K2HDKC_DBGFILE"); f != "" {
		file = f
		fp, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("[%v] %v open error:[%s]. Fallback to stderr.", logSeverityText[SeverityError], file, err)
			fp = os.Stderr
		}
		logger := log.New(fp, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
		l := &K2hLog{
			file:        file,
			fp:          fp,
			logger:      logger, // l.logger refers logger.
			severity:    severity,
			libSeverity: defaultLibLogSeverity,
		}
		return l
	}
	// 2009/01/23 01:23:23.123123 sample.go:10: message
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	l := &K2hLog{
		file:         "",
		fp:           os.Stderr,
		logger:       logger, // l.logger refers logger.
		severity:     severity,
		libSeverity:  defaultLibLogSeverity,
		bundleLibLog: defaultBundleLibLog,
	}
	return l
}

// Close closes the file descriptor.
// NOTICE You must call Close() to avoid leaking file descriptor.
func (l *K2hLog) Close() {
	if l.fp != nil {
		l.fp.Close()
	}
}

// String returns a text representation of the object.
func (l *K2hLog) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v]", l.file, l.fp, l.severity, l.libSeverity, l.bundleLibLog)
}

// BundleLibLog sets the log file for dependent libraries.
func (l *K2hLog) BundleLibLog(b bool) {
	if l.file != "" && b {
		cs := C.CString(l.file)
		defer C.free(unsafe.Pointer(cs))
		C.k2hdkc_set_debug_file(cs)
		C.chmpx_set_debug_file(cs)
		C.k2h_set_debug_file(cs)
	} else {
		C.k2hdkc_unset_debug_file()
		C.chmpx_unset_debug_file()
		C.k2h_unset_debug_file()
	}
}

// SetLogFile sets the file to be logged.
func (l *K2hLog) SetLogFile(f string) {
	if f != "" {
		fp, err := os.OpenFile(f, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("[%v] %v open error:[%s]. Fallback to stderr.", logSeverityText[SeverityError], f, err)
			fp = os.Stderr
		}
		logger := log.New(fp, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
		if logger != nil {
			// new logger comes up. closing existing logger.
			l.Close()
		}
		l.file = f
		l.fp = fp
		l.logger = logger
		l.BundleLibLog(true)
	} else {
		logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
		if logger != nil {
			// new logger comes up. closing existing logger.
			l.Close()
		}
		l.file = f
		l.fp = os.Stderr
		l.logger = logger
		l.BundleLibLog(false) // redirect logging to stderr.
	}
}

// normalizeSeverity nomalizes the specified string to a adequate enum severity.
func normalizeSeverity(p string) logSeverity {
	switch p {
	case "SLT", "SILENT":
		return SeveritySilent
	case "ERR", "ERROR":
		return SeverityError
	case "WAN", "WARNING":
		return SeverityWarning
	case "MSG", "INFO":
		return SeverityInfo
	case "DMP", "DUMP":
		return SeverityDump
	default:
		// It's not an error, but a warning.
		log.Printf("[%v] Unknown severity. Fallback to ERROR.", logSeverityText[SeverityError])
		return defaultLogSeverity
	}
}

// SetLogSeverity sets the severity member.
func (l *K2hLog) SetLogSeverity(p logSeverity) {
	l.severity = p
}

// SetLibLogSeverity enables logging of the dependent libraries.
func (l *K2hLog) SetLibLogSeverity(p logSeverity) {
	if (p & LibComlog) != 0 {
		l.SetComlog(true)
	}
	if (p & LibK2hdkc) != 0 {
		l.SetK2hdkcLog(true)
	}
	if (p & LibChmpx) != 0 {
		l.SetChmpxLog(true)
	}
	if (p & LibK2hash) != 0 {
		l.SetK2hashLog(true)
	}
	// reset
	if p == LibSeveritySilent {
		l.SetComlog(false)
		l.SetK2hdkcLog(false)
		l.SetChmpxLog(false)
		l.SetK2hashLog(false)
	}
}

// SetComlog enables the k2hdkc communication logging.
func (l *K2hLog) SetComlog(b bool) {
	if b {
		switch l.severity {
		case SeveritySilent:
			C.k2hdkc_disable_comlog()
		default:
			C.k2hdkc_enable_comlog()
		}
	} else {
		C.k2hdkc_disable_comlog()
	}
}

// SetK2hdkcLog sets the serverity of k2hdkc library logger.
func (l *K2hLog) SetK2hdkcLog(b bool) {
	if b {
		switch l.severity {
		case SeveritySilent:
			C.k2hdkc_set_debug_level_silent() // set silent for debugging level
		case SeverityError:
			C.k2hdkc_set_debug_level_error() // set error for debugging level
		case SeverityWarning:
			C.k2hdkc_set_debug_level_warning() // set warning for debugging level
		case SeverityInfo:
			C.k2hdkc_set_debug_level_message() // set message for debugging level
		case SeverityDump:
			C.k2hdkc_set_debug_level_dump() // set dump for debugging level
		default:
			log.Printf("[%v] Unknown severity. Fallback to ERROR.", logSeverityText[SeverityError])
			C.k2hdkc_set_debug_level_message() // set message for debugging leve
		}
	} else {
		C.k2hdkc_set_debug_level_silent() // set silent for debugging level
	}
}

// SetChmpxLog sets the serverity of chmpx library logger.
func (l *K2hLog) SetChmpxLog(b bool) {
	if b {
		switch l.severity {
		case SeveritySilent:
			C.chmpx_set_debug_level_silent() // set silent for debugging level
		case SeverityError:
			C.chmpx_set_debug_level_error() // set error for debugging level
		case SeverityWarning:
			C.chmpx_set_debug_level_warning() // set warning for debugging level
		case SeverityInfo:
			C.chmpx_set_debug_level_message() // set message for debugging level
		case SeverityDump:
			C.chmpx_set_debug_level_dump() // set dump for debugging level
		default:
			log.Printf("[%v] Unknown severity. Fallback to ERROR.", logSeverityText[SeverityError])
			C.chmpx_set_debug_level_message() // set message for debugging leve
		}
	} else {
		C.chmpx_set_debug_level_silent() // set silent for debugging level
	}
}

// SetK2hashLog sets the serverity of k2hash library logger.
func (l *K2hLog) SetK2hashLog(b bool) {
	if b {
		switch l.severity {
		case SeveritySilent:
			C.k2h_set_debug_level_silent() // set silent for debugging level
		case SeverityError:
			C.k2h_set_debug_level_error() // set error for debugging level
		case SeverityWarning:
			C.k2h_set_debug_level_warning() // set warning for debugging level
		case SeverityInfo, SeverityDump:
			C.k2h_set_debug_level_message() // set message for debugging level
		default:
			log.Printf("[%v] Unknown severity. Fallback to ERROR.", logSeverityText[SeverityError])
			C.k2h_set_debug_level_message() // set message for debugging leve
		}
	} else {
		C.k2h_set_debug_level_silent() // set silent for debugging level
	}
}

// Dump prints a debug message.
func (l *K2hLog) Dump(msg string) {
	if l.severity&(SeverityDump) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityDump], msg)
	}
}

// Dumpf prints a formatted debug message.
func (l *K2hLog) Dumpf(format string, v ...interface{}) {
	if l.severity&(SeverityDump) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityDump], fmt.Sprintf(format, v...))
	}
}

// Info prints a normal severity message.
func (l *K2hLog) Info(msg string) {
	if l.severity&(SeverityDump|SeverityInfo) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityInfo], msg)
	}
}

// Infof prints a formatted normal severity message.
func (l *K2hLog) Infof(format string, v ...interface{}) {
	if l.severity&(SeverityDump|SeverityInfo) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityInfo], fmt.Sprintf(format, v...))
	}
}

// Error prints an error message.
func (l *K2hLog) Error(msg string) {
	if l.severity&(SeverityDump|SeverityInfo|SeverityError) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityError], msg)
	}
}

// Errorf prints a formatted error message.
func (l *K2hLog) Errorf(format string, v ...interface{}) {
	if l.severity&(SeverityDump|SeverityInfo|SeverityError) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityError], fmt.Sprintf(format, v...))
	}
}

// Warn prints a warning message.
func (l *K2hLog) Warn(msg string) {
	if l.severity&(SeverityDump|SeverityInfo|SeverityError|SeverityWarning) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityWarning], msg)
	}
}

// Warnf prints a formatted warning message.
func (l *K2hLog) Warnf(format string, v ...interface{}) {
	if l.severity&(SeverityDump|SeverityInfo|SeverityError|SeverityWarning) != 0 {
		l.logger.Printf("[%v] %v", logSeverityText[SeverityWarning], fmt.Sprintf(format, v...))
	}
}

// SetOutput sets the destination for the logger.
func (l *K2hLog) SetOutput(w io.Writer) {
	l.logger.SetOutput(w)
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
