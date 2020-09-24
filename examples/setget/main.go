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

package main

import (
	"fmt"
	"os"

	"github.com/yahoojapan/k2hdkc_go/k2hdkc"
)

func setAndGet() {
	c := k2hdkc.NewClient("../../cluster/slave.yaml", 8031)
	defer c.Close()
	// save a hello key.
	if r, err := c.Set("hello", "world"); r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.Set(key, value) returned r %v err %v\n", r, err)
		return
	}
	// get the key.
	r, err := c.Get("hello")
	if r == nil || err != nil {
		fmt.Fprintf(os.Stderr, "client.Get(key) returned r %v err %v\n", r, err)
		return
	}
	fmt.Println(r.String())
}

func main() {
	setAndGet()
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
