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
	// #cgo CFLAGS: -g -O2 -Wall -Wextra -Wno-unused-variable  -Wno-unused-parameter -I. -I/usr/include/k2hdkc
	// #cgo LDFLAGS: -L/usr/lib -lk2hdkc
	// #include "k2hdkc.h"
	"C"
)
import (
	"fmt"
	"os"
)

// Client holds settings to connect a chmpx slave server.
// Client is responsible for logging messages to a log file.
// Client is not responsible for managing request handlers with a k2hdkc cluster.
type Client struct {
	file        string // the configuration of the chmpx
	port        uint16 // the control port number of the chmpx
	cuk         string // the cloud unique key string of the chmpx
	rejoin      bool   // reconnect automatically when the connection with the chmpx
	rejoinRetry bool   // retry count to reconnect automatically to the chmpx
	cleanup     bool   // delete the unnecessary information file when leaving.
	log         *K2hLog
}

// NewClient returns the pointer to a Client after initializing members.
func NewClient(f string, p uint16) *Client {
	if unSupportedOs {
		fmt.Fprintf(os.Stderr, "k2hdkc currently works on linux only")
		return nil
	}
	if unSupportedEndian {
		fmt.Fprintf(os.Stderr, "k2hdkc_go currently works on little endian alignment only")
		return nil
	}
	if isNotExistLibK2hdkc {
		fmt.Fprintf(os.Stderr, "Please install the k2hdkc package at first")
		return nil
	}
	log := K2hLogInstance()
	return &Client{
		file:        f,
		port:        p,
		cuk:         defaultCuk,
		rejoin:      defaultAutoRejoin,
		rejoinRetry: defaultAutoRejoinRetry,
		cleanup:     defaultCleanup,
		log:         log,
	}
}

// String returns a text representation of the object.
func (c *Client) String() string {
	return fmt.Sprintf("[%v, %v, %v, %v, %v, %v]", c.file, c.port, c.rejoin, c.rejoinRetry, c.cleanup, c.log)
}

// CreateSession returns the pointer to a session with chmpx which handler is open..
func (c *Client) CreateSession() (*Session, error) {
	if _, err := os.Stat(c.file); os.IsNotExist(err) {
		return nil, fmt.Errorf("no %v exists", c.file)
	}
	s, err := NewSession(c)
	if s != nil {
		defer s.Close()
		return s, nil
	}
	return nil, fmt.Errorf("creating a session: %v", err)
}

// SetChmpxFile sets a chmpx file.
func (c *Client) SetChmpxFile(f string) *Client {
	c.file = f
	return c
}

// SetCtlPort sets a control port.
func (c *Client) SetCtlPort(p uint16) *Client {
	c.port = p
	return c
}

// SetCuk sets a cuk string.
func (c *Client) SetCuk(u string) *Client {
	c.cuk = u
	return c
}

// SetAutoRejoin sets a rejoin member flag.
func (c *Client) SetAutoRejoin(b bool) *Client {
	c.rejoin = b
	return c
}

// SetAutoRejoinRetry sets a rejoinRetry member flag.
func (c *Client) SetAutoRejoinRetry(b bool) *Client {
	c.rejoinRetry = b
	return c
}

// SetCleanup sets a cleanup member flag.
func (c *Client) SetCleanup(b bool) *Client {
	c.cleanup = b
	return c
}

// SetLogger sets a new logger.
func (c *Client) SetLogger(l *K2hLog) *Client {
	// Assuming user want to assign a new logger.
	if c.log != nil {
		c.log.Close()
	}
	if l != nil {
		c.log = l
	} else {
		c.log = K2hLogInstance()
	}
	return c
}

// SetLogFile changes the logger output file.
func (c *Client) SetLogFile(f string) *Client {
	if c.log == nil {
		c.log = K2hLogInstance()
	}
	c.log.SetLogFile(f)
	return c
}

// SetLogSeverity changes the log serverity of k2hdkc_go and it's libraries.
func (c *Client) SetLogSeverity(p logSeverity) *Client {
	if c.log == nil {
		c.log = K2hLogInstance()
	}
	c.log.SetLogSeverity(p)
	c.log.SetLibLogSeverity(LibK2hdkc)
	return c
}

// SetLibLogSeverity changes the log serverity of libraries.
func (c *Client) SetLibLogSeverity(p logSeverity) *Client {
	if c.log == nil {
		c.log = K2hLogInstance()
	}
	c.log.SetLibLogSeverity(p)
	return c
}

// Close calls the K2hLogger.Close().
// NOTICE You must call Close() to avoid leaking file descriptor.
func (c *Client) Close() {
	if c.log != nil {
		c.log.Close()
	}
	return
}

// Send returns a pointer of a Command.
func (c *Client) Send(cmd Command) (Command, error) {
	if cmd != nil {
		s, err := NewSession(c)
		if s != nil {
			defer s.Close()
		}
		if err != nil {
			return nil, fmt.Errorf("failed to create a session. %v", err)
		}

		ok, err := cmd.Execute(s)
		if !ok || err != nil {
			return nil, fmt.Errorf("cmd.Execute(s) returned ok %v err %v", ok, err)
		}

		return cmd, err
	}
	return nil, fmt.Errorf("cmd is %v", nil)
}

// Set returns a pointer of SetResult.
func (c *Client) Set(k string, v string) (*SetResult, error) {
	cmd, err := NewSet(k, v)
	if err != nil {
		return nil, fmt.Errorf("NewSet(k, v) returned err %v", err)
	}

	s, err := NewSession(c)
	if s != nil {
		defer s.Close()
	} else {
		return nil, fmt.Errorf("failed to create a session. %v", err)
	}

	if ok, err := cmd.Execute(s); !ok {
		c.log.Warnf("NewSet.Execute(s) returned ok %v err %v", ok, err)
		return cmd.result, err
	}
	return cmd.result, nil
}

// Get returns a pointer of GetResult.
func (c *Client) Get(k string) (*GetResult, error) {
	cmd, err := NewGet(k)
	if err != nil {
		return nil, fmt.Errorf("NewGet(k, v) returned %v", err)
	}

	s, err := NewSession(c)
	if s != nil {
		defer s.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create a session. %v", err)
	}

	if ok, err := cmd.Execute(s); !ok {
		c.log.Warnf("NewGet.Execute(s) returned ok %v err %v", ok, err)
		return cmd.result, err
	}
	return cmd.Result(), nil
}

// GetSubKeys returns a pointer of GetSubKeysResult.
func (c *Client) GetSubKeys(k string) (*GetSubKeysResult, error) {
	cmd, err := NewGetSubKeys(k)
	if err != nil {
		return nil, fmt.Errorf("NewGetSubKeys(k) returned %v", err)
	}

	s, err := NewSession(c)
	if s != nil {
		defer s.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create a session. %v", err)
	}

	if ok, err := cmd.Execute(s); !ok {
		c.log.Warnf("NewGetSubKeys.Execute(s) returned ok %v err %v", ok, err)
		return cmd.result, err
	}
	return cmd.result, nil
}

// SetSubKeys returns a pointer of SetSubKeysResult.
func (c *Client) SetSubKeys(k string, skeys []string) (*SetSubKeysResult, error) {
	cmd, err := NewSetSubKeys(k, skeys)
	if err != nil {
		return nil, fmt.Errorf("NewSetSubKeys(k, skeys) returned %v", err)
	}

	s, err := NewSession(c)
	if s != nil {
		defer s.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create a session. %v", err)
	}

	if ok, err := cmd.Execute(s); !ok {
		c.log.Warnf("NewSetSubKeys.Execute(s) returned ok %v err %v", ok, err)
		return cmd.result, err
	}
	return cmd.result, nil
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
