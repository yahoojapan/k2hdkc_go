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
	"bufio"
	"bytes"
	"fmt"
	"os"
	"regexp"
	"testing"
)

// ensureLog read data up to 128bytes from the path and match the data with the want.
func ensureLog(t *testing.T, path string, want string) {
	// 1. open the log if exists.
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		t.Errorf("os.Open(path) = %v", err)
	}

	// 2. read the file and ensure it contains the want. maxlength 128 is enough.
	// 2018/03/08 11:29:22.651207 log.go:304: [WARN] warn occurred.
	data := make([]byte, 128)
	n, err := f.Read(data)
	if err != nil {
		t.Errorf("f.Read(%q) = (%v, %v)", data, n, err)
	}
	buf := bytes.NewReader(data)
	l := bufio.NewReaderSize(buf, n)
	line, isPrefix, err := l.ReadLine()
	if isPrefix || err != nil {
		t.Errorf("l.ReadLine() = (%v, %v,  %v)", line, isPrefix, err)
	}
	rwant := fmt.Sprintf("^.* \\[%s\\] %s$", logSeverityText[SeverityError], want)
	matched, err := regexp.MatchString(rwant, string(line))
	if !matched || err != nil {
		// %q means a single-quoted character literal safely escaped with Go syntax.
		t.Errorf("regexp.MatchString(%q, %q) = (%v, %v)", rwant, string(line), matched, err)
	}
}

// TestK2hLogEnvFileCreateFile set the log file from the GO_K2HDKC_DBGFILE env.
// At first, the log file doesn't exist and logger should create it.
// The ensureLog function check the want exists in the log file.
func TestK2hLogEnvFileCreateFile(t *testing.T) {
	// 1. remove the log if exists.
	path := "test.log"
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err == nil || os.IsNotExist(err) {
			//fmt.Fprintf(os.Stderr, "path %v removed", path)
		}
	}

	// 2. setenv GO_K2HDKC_DBGFILE and start logging and close log.
	os.Setenv("GO_K2HDKC_DBGFILE", path)

	// Calling newK2hLog() to test the environments.
	// Because K2hLogInstance() will check environments only initializing.
	log := newK2hLog()
	want := "error occurred."
	log.Error(want)
	log.Close()

	ensureLog(t, path, want)
}

// TestK2hLogEnvFileAppendFile set the log file from the GO_K2HDKC_DBGFILE env.
// Sometime, the log file already exists and logger should append log to it.
// The ensureLog function check the want exists in the log file.
func TestK2hLogEnvFileAppendFile(t *testing.T) {
	path := "test.log"
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Errorf("os.OpenFile(%v) = (%v, %v)", path, f, err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("f.Close() = %v", err)
	}

	os.Setenv("GO_K2HDKC_DBGFILE", path)
	// Calling newK2hLog() to test the environments.
	// Because K2hLogInstance() will check environments only initializing.
	log := newK2hLog()
	want := "error occurred."
	log.Error(want)
	log.Close()

	ensureLog(t, path, want)
}

// ensureLogBuffer set bytes.Buffer to the log library.
// which checks if the bytes.Buffer contains the log.
func ensureLogBuffer(t *testing.T, log *K2hLog, want string) {
	var buf bytes.Buffer
	log.SetOutput(&buf) // bytes.Buffer implements io.Writer
	log.Error(want)     // log want

	str := buf.String()
	if len(str) == 0 {
		t.Errorf("len(%q) = %v, want != 0", str, len(str))
	}
	str = str[0 : len(str)-1] // remove CR
	rwant := fmt.Sprintf("^.* \\[%s\\] %s$", logSeverityText[SeverityError], want)
	matched, err := regexp.MatchString(rwant, str)
	if !matched || err != nil {
		t.Errorf("regexp.MatchString(%q, %q) = (%v, %v)", rwant, str, matched, err)
	}
	log.SetOutput(os.Stderr)
}

// TestK2hLogSetLogFile checks if the log file contains the log.
// If the log file is empty, stderr is the log file.
func TestK2hLogSetLogFile(t *testing.T) {
	// 1. remove the log if exists.
	path := "test.log"
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err == nil || os.IsNotExist(err) {
			//fmt.Fprintf(os.Stderr, "path %v removed", path)
		}
	}
	// 2. log want to the file.
	log := K2hLogInstance()
	log.SetLogFile(path)
	want := "error occurred."
	log.Error(want)

	// 3. check the log file.
	ensureLog(t, path, want)

	// 4. log want to stderr.
	log.SetLogFile("")

	// 5. check the buffer.
	ensureLogBuffer(t, log, want)
	log.Close()
}

// TestK2hLogEnvSeverity checks if severity has changed.
// TestK2hLogEnvSeverity places bytes.Buffer to log library and starts logging.
// If the GO_K2HDKC_DBGLEVEL is SILENT, bytes.Buffer contains no log because now it's silent.
// If not, the bytes.Buffer shold contain the log.
func TestK2hLogEnvSeverity(t *testing.T) {
	os.Setenv("GO_K2HDKC_DBGLEVEL", "SILENT")
	// Calling newK2hLog() to test the environments.
	// Because K2hLogInstance() will check environments only initializing.
	log := newK2hLog()
	var buf bytes.Buffer
	log.SetOutput(&buf) // bytes.Buffer implements io.Writer
	log.Error("error occurred.")
	str := buf.String()
	if len(str) != 0 {
		t.Errorf("len(%q) = %v, want != 0", str, len(str))
	}
	log.SetOutput(os.Stderr)
	log.Close()
}

// TestK2hLogEnvSeverityInvalid sets invalid severity.
func TestK2hLogEnvSeverityInvalid(t *testing.T) {
	os.Setenv("GO_K2HDKC_DBGLEVEL", "INVALID")
	// Calling newK2hLog() to test the environments.
	// Because K2hLogInstance() will check environments only initializing.
	log := newK2hLog()
	want := "error occurred."
	ensureLogBuffer(t, log, want)
	log.Close()
}

// TestK2hLogSeverity checks priorities and functions.
// If the severity is INFO and log.Error() is called, message should not be logged.
func TestK2hLogSeverity(t *testing.T) {
	os.Unsetenv("GO_K2HDKC_DBGLEVEL")
	os.Unsetenv("GO_K2HDKC_DBGFILE")

	priorities := []logSeverity{SeveritySilent, SeverityError, SeverityWarning, SeverityInfo, SeverityDump}
	for _, p := range priorities {
		log := K2hLogInstance()
		var buf bytes.Buffer
		log.SetOutput(&buf) // bytes.Buffer implements io.Writer
		log.SetLogSeverity(p)
		//fmt.Fprintf(os.Stderr, "severity %v", logSeverityText[p])

		msg := fmt.Sprintf("msg %s", logSeverityText[p])
		want := fmt.Sprintf("^.* \\[%s\\] %s$", logSeverityText[p], msg)
		switch p {
		case SeveritySilent:
			log.Error(msg) // should be empty even if log.Error(msg).
		case SeverityError:
			log.Error(msg)
		case SeverityWarning:
			log.Warn(msg)
		case SeverityInfo:
			log.Info(msg)
		case SeverityDump:
			log.Dump(msg)
		default:
			fmt.Fprintf(os.Stderr, "[%v] Unknown severity. Fallback to ERROR.", logSeverityText[SeverityError])
			log.Error(msg)
		}

		str := buf.String()
		if len(str) == 0 {
			if p != SeveritySilent {
				t.Errorf("len(%q) = %v, want != 0", str, len(str))
			}
		} else {
			str = str[0 : len(str)-1] // remove CR
			//fmt.Fprintf(os.Stderr, "str=[%v]\n", str)
			matched, err := regexp.MatchString(want, str)
			if !matched || err != nil {
				t.Errorf("regexp.MatchString(%q, %q) = (%v, %v)", want, str, matched, err)
			}
		}
		log.SetOutput(os.Stderr)
		log.Close()
	}
}

func testK2hLogBundleLibLog(t *testing.T) {
	// 1. remove the log if exists.
	path := "test.log"
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err == nil || os.IsNotExist(err) {
			//fmt.Fprintf(os.Stderr, "path %v removed", path)
		}
	}
	// 2. log want to the file.
	log := K2hLogInstance()
	log.SetLogFile(path)

	// 3. call the BundleLibLog method.
	log.BundleLibLog(true)
	want := "error occurred."
	log.Error(want)

	// 3. check the log file.
	ensureLog(t, path, want)
	log.Close()
}

func TestK2hLogBundleLibLogUnset(t *testing.T) {
	log := K2hLogInstance()
	log.BundleLibLog(true)
	log.BundleLibLog(false)
	log.Close()
}

func TestK2hLogSetLibLogSeverity(t *testing.T) {
	log := K2hLogInstance()
	log.SetLibLogSeverity(LibComlog | LibK2hdkc | LibChmpx | LibK2hash)
	log.Close()
}

func TestK2hLogSetLibLogSeverityUnset(t *testing.T) {
	log := K2hLogInstance()
	log.SetLibLogSeverity(LibSeveritySilent)
	log.Close()
}

// TestK2hLogFormat calls log.Errorf, log.Warnf, log.Infof, log.Dumpf functions.
func TestK2hLogFormat(t *testing.T) {
	os.Unsetenv("GO_K2HDKC_DBGLEVEL")
	os.Unsetenv("GO_K2HDKC_DBGFILE")

	priorities := []logSeverity{SeveritySilent, SeverityError, SeverityWarning, SeverityInfo, SeverityDump}
	for _, p := range priorities {
		log := K2hLogInstance()
		var buf bytes.Buffer
		log.SetOutput(&buf) // bytes.Buffer implements io.Writer
		log.SetLogSeverity(p)
		//fmt.Fprintf(os.Stderr, "severity %v", logSeverityText[p])

		msg := fmt.Sprintf("msg %s", logSeverityText[p])
		want := fmt.Sprintf("^.* \\[%s\\] %s$", logSeverityText[p], msg)
		switch p {
		case SeveritySilent:
			log.Errorf("%s", msg) // should be empty even if log.Error(msg).
		case SeverityError:
			log.Errorf("%s", msg)
		case SeverityWarning:
			log.Warnf("%s", msg)
		case SeverityInfo:
			log.Infof("%s", msg)
		case SeverityDump:
			log.Dumpf("%s", msg)
		default:
			fmt.Fprintf(os.Stderr, "[%v] Unknown severity. Fallback to ERROR.", logSeverityText[SeverityError])
			log.Errorf("%s", msg)
		}

		str := buf.String()
		if len(str) == 0 {
			if p != SeveritySilent {
				t.Errorf("len(%q) = %v, want != 0", str, len(str))
			}
		} else {
			str = str[0 : len(str)-1] // remove CR
			//fmt.Fprintf(os.Stderr, "str=[%v]\n", str)
			matched, err := regexp.MatchString(want, str)
			if !matched || err != nil {
				t.Errorf("regexp.MatchString(%q, %q) = (%v, %v)", want, str, matched, err)
			}
		}
		log.SetOutput(os.Stderr)
		log.Close()
	}
}

func TestK2hLogInstance(t *testing.T) {
	logger := K2hLogInstance()
	if logger == nil {
		t.Errorf("K2hLogInstance() == %v", logger)
	}
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
