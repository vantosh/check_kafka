/*
 * check_kafka
 *
 * (c) Copyright 2024 VanTosh, all rights reserverd
 * Author : Toshaan Bharvani <toshaan@vantosh.com
 *
 * Use of this source code is governed by a Apache 2.0 license,
 * that can be found in the LICENSE file.
 *
 */

package version

import (
	"fmt"
)

var Version string
var GitRevision string
var GitBranch string
var Builder string
var Client string

func PrintVersion() {
	fmt.Printf("Check Kafka\n")
	fmt.Printf("========================================\n")
	fmt.Printf("License: Apache v2\n")
	fmt.Printf("Version: %s\n", Version)
	fmt.Printf("  git : (%s) %s\n", GitBranch, GitRevision)
	fmt.Printf("Builder: %s\n", Builder)
	fmt.Printf("Client: %s\n", Client)
	fmt.Printf("========================================\n")
}
