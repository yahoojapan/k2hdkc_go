# k2hdkc_go

### Overview

**k2hdkc_go** implements a [k2hdkc](https://k2hdkc.antpick.ax/) client in golang.

### Install

Firstly you must install the [k2hdkc](https://k2hdkc.antpick.ax/) shared library.
```
$ curl -o- https://raw.github.com/yahoojapan/k2hdkc_go/master/cluster/start_server.sh | bash
```
You can install **k2hdkc** library step by step from [source code](https://github.com/yahoojapan/k2hdkc). See [Build](https://k2hdkc.antpick.ax/build.html) for details.

After you make sure you set the [GOPATH](https://github.com/golang/go/wiki/SettingGOPATH) environment, download the **k2hdkc_go** package.
```
$ go get -u github.com/yahoojapan/k2hdkc_go
```

### Usage

Here is a simple example of **k2hdkc_go** which save a key and get it.

```golang
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
```

### Development

Here is the step to start developing **k2hdkc_go**.

- Debian / Ubuntu

```bash
#!/bin/sh

sudo apt-get update -y && sudo apt-get install curl git -y && curl -s https://packagecloud.io/install/repositories/antpickax/stable/script.deb.sh | sudo bash
sudo apt-get install libfullock-dev k2hash-dev chmpx-dev k2hdkc-dev -y
go get github.com/yahoojapan/k2hdkc_go/k2hdkc

exit 0
```

- CentOS / Fedora

```bash
#!/bin/sh

sudo dnf makecache && sudo dnf install curl git -y && curl -s https://packagecloud.io/install/repositories/antpickax/stable/script.rpm.sh | sudo bash
sudo dnf install libfullock-devel k2hash-devel chmpx-devel k2hdkc-devel -y
go get github.com/yahoojapan/k2hdkc_go/k2hdkc

exit 0
```

### Documents
  - [About k2hdkc](https://k2hdkc.antpick.ax/)
  - [About AntPickax](https://antpick.ax/)

### License

MIT License. See the LICENSE file.

## AntPickax

[AntPickax](https://antpick.ax/) is 
  - an open source team in [Yahoo Japan Corporation](https://about.yahoo.co.jp/info/en/company/). 
  - a product family of open source software developed by [AntPickax](https://antpick.ax/).

