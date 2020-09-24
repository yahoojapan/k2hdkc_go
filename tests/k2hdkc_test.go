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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"unsafe"
)

// TestMain is the main function of testing.
func TestMain(m *testing.M) {
	if runtime.GOOS != "linux" && runtime.GOARCH != "amd64" {
		fmt.Fprintf(os.Stderr, "k2hdkc currently works on linux only")
		os.Exit(-1)
	}
	i := uint32(1)
	b := (*[4]byte)(unsafe.Pointer(&i))
	if b[0] != 1 {
		fmt.Fprintf(os.Stderr, "k2hdkc_go currently works on little endian alignment only")
		os.Exit(-1)
	}
	// rpm or deb install the k2hdkc.so in /usr/lib or /usr/lib64
	if _, err := os.Stat("/usr/lib/libk2hdkc.so"); err != nil {
		if _, err := os.Stat("/usr/lib64/libk2hdkc.so"); err != nil {
			fmt.Fprintf(os.Stderr, "Please install the k2hdkc package at first")
			os.Exit(-1)
		}
	}
	//setUp(m)
	status := m.Run()
	//tearDown(m)
	os.Exit(status)
}

func setUp(m *testing.M) {
	fmt.Println("test.go setUp")
	if stopCluster == false {
		cmd := exec.Command("sh", "-c", "ps axuww|grep test.run=|grep -v grep")
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Fprintf(os.Stderr, "cmd.StdoutPipe() %v", err)

		}
		if err = cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "cmd.StdoutPipe() %v", err)
		}
		slurp, _ := ioutil.ReadAll(stdout)
		if len(slurp) != 0 {
			fmt.Printf("Test scope is only for this type. %s\n", slurp)
			stopCluster = true
			if err = cmd.Wait(); err != nil {
				fmt.Fprintf(os.Stderr, "cmd.Wait() %v pid %v", err, os.Getpid())
			}
		} else {
			fmt.Printf("Test scope is for all types. %s\n", slurp)
			cmd.Wait()
		}

		start := exec.Command("./cluster/k2hdkc.sh", "startifnotexist")
		fmt.Println("Starting a k2hdkc cluster...")
		err = start.Run()
		if err != nil {
			fmt.Fprintf(os.Stderr, "./cluster/k2hdkc.sh startifnotexist finished with error %v", err)
		}
	}

}

func tearDown(m *testing.M) {
	if stopCluster {
		fmt.Println("Stopping the k2hdkc cluster...")
		stop := exec.Command("./cluster/k2hdkc.sh", "stop")
		err := stop.Run()
		if err != nil {
			fmt.Fprintf(os.Stderr, "./cluster/k2hdkc.sh stop finished with error %v", err)
		}
	} else {
		fmt.Println("No need to stop the k2hdkc cluster...")
	}

}

func TestAddSubKeyAPI(t *testing.T)                     { testAddSubKey(t) }
func TestAddSubKeyTypeStringEmptyAPI(t *testing.T)      { testAddSubKeyTypeStringEmpty(t) }
func TestAddSubKeyKeyTypeUnknownAPI(t *testing.T)       { testAddSubKeyKeyTypeUnknown(t) }
func TestCasGetAPI(t *testing.T)                        { testCasGet(t) }
func TestCasIncDecAPI(t *testing.T)                     { testCasIncDec(t) }
func TestCasInitAPI(t *testing.T)                       { testCasInit(t) }
func TestCasSetAPI(t *testing.T)                        { testCasSet(t) }
func TestClearSubKeysTypeByteAPI(t *testing.T)          { testClearSubKeysTypeByte(t) }
func TestClearSubKeysTypeStringEmptyAPI(t *testing.T)   { testClearSubKeysTypeStringEmpty(t) }
func TestClearSubKeysKeyTypeUnknownAPI(t *testing.T)    { testClearSubKeysKeyTypeUnknown(t) }
func TestClientAPI(t *testing.T)                        { testClient(t) }
func TestClientCreateSessionAPI(t *testing.T)           { testClientCreateSession(t) }
func TestClientCreateSessionErrorAPI(t *testing.T)      { testClientCreateSessionError(t) }
func TestClientSetMethodsAPI(t *testing.T)              { testClientSetMethods(t) }
func TestClientSetAndGetAPI(t *testing.T)               { testClientSetAndGet(t) }
func TestClientSetSubKeysAndGetSubKeysAPI(t *testing.T) { testClientSetSubKeysAndGetSubKeys(t) }
func TestGetAttrsTypeByteAPI(t *testing.T)              { testGetAttrsTypeByte(t) }
func TestGetAPI(t *testing.T)                           { testGet(t) }
func TestGetTypeStringEmptyAPI(t *testing.T)            { testGetTypeStringEmpty(t) }
func TestGetKeyTypeUnknownAPI(t *testing.T)             { testGetKeyTypeUnknown(t) }
func TestGetSubKeysAPI(t *testing.T)                    { testGetSubKeys(t) }
func TestGetSubKeysTypeStringEmptyAPI(t *testing.T)     { testGetSubKeysTypeStringEmpty(t) }
func TestGetSubKeysKeyTypeUnknownAPI(t *testing.T)      { testGetSubKeysKeyTypeUnknown(t) }
func TestQueuePopAPI(t *testing.T)                      { testQueuePop(t) }
func TestQueuePushAPI(t *testing.T)                     { testQueuePush(t) }
func TestQueueRemoveAPI(t *testing.T)                   { testQueueRemove(t) }
func TestRemoveTypeByte(t *testing.T)                   { testRemoveTypeByte(t) }
func TestRemoveTypeStringEmptyAPI(t *testing.T)         { testRemoveTypeStringEmpty(t) }
func TestRemoveKeyTypeUnknownAPI(t *testing.T)          { testRemoveKeyTypeUnknown(t) }
func TestRemoveSubKeyAPI(t *testing.T)                  { testRemoveSubKey(t) }
func TestRemoveSubKeyTypeStringEmptyAPI(t *testing.T)   { testRemoveSubKeyTypeStringEmpty(t) }
func TestRemoveSubKeyKeyTypeUnknownAPI(t *testing.T)    { testRemoveSubKeyKeyTypeUnknown(t) }
func TestRenameAPI(t *testing.T)                        { testRename(t) }
func TestRenameParentAPI(t *testing.T)                  { testRenameParent(t) }
func TestSessionNewAPI(t *testing.T)                    { testSessionNew(t) }
func TestSessionNewErrorAPI(t *testing.T)               { testSessionNewError(t) }
func TestSessionCreateAPI(t *testing.T)                 { testSessionCreate(t) }
func TestSetAllAPI(t *testing.T)                        { testSetAll(t) }
func TestSetAllTypeStringEmptyAPI(t *testing.T)         { testSetAllTypeStringEmpty(t) }
func TestSetAllKeyTypeUnknownAPI(t *testing.T)          { testSetAllKeyTypeUnknown(t) }
func TestSetAPI(t *testing.T)                           { testSet(t) }
func TestSetTypeUnknownAPI(t *testing.T)                { testSetTypeUnknown(t) }
func TestSetTypeEmptyAPI(t *testing.T)                  { testSetTypeEmpty(t) }
func TestSetSubKeysAPI(t *testing.T)                    { testSetSubKeys(t) }
func TestSetSubKeysTypeEmptyAPI(t *testing.T)           { testSetSubKeysTypeEmpty(t) }

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
