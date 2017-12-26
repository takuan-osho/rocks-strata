//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

package main

import (
	"os"
	"strings"

	"github.com/takuan-osho/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrossdriver"

	"github.com/facebookgo/rocks-strata/strata/cmd/mongo/lreplica_drivers/lrs3driver"
	"github.com/facebookgo/rocks-strata/strata/mongo"
)

func main() {
	switch strings.ToLower(os.Getenv("REMOTE_STORAGE")) {
	case "oss":
		mongoq.RunCLI(lrossdriver.DriverFactory{Ops: &lrossdriver.Options{}})
	default:
		mongoq.RunCLI(lrs3driver.DriverFactory{Ops: &lrs3driver.Options{}})
	}
}
