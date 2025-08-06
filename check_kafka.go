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

package main

import (
	"fmt"
	"os"
	"flag"
	"check_kafka/functions"
	"check_kafka/version"
)

func main() {
	commandFlag := flag.String("command", "", "Command to execute")
	clusterFlag := flag.String("cluster", "", "Cluster Bootstrap Servers")
	usernameFlag := flag.String("username", "", "Username")
	passwordFlag := flag.String("password", "", "Password")
	authmethodFlag := flag.String("mechanism", "PLAIN", "SASL Mechanism")
	protocolFlag := flag.String("security-protocol", "SASL_SSL", "Security Protocol")
	topicFlag := flag.String("topic", "", "Kafka Topic")
	flag.Parse()

	switch *commandFlag {
	case "version":
		fmt.Printf("Check Kafka\nLicense: Apache v2\nVersion: %s\ngit : (%s) %s\nBuilder: %s\nClient: %s\n", version.Version, version.GitBranch, version.GitRevision, version.Builder, version.Client)
		os.Exit(0)
	case "describecluster":
		kafkaConnection := functions.Connect(*clusterFlag, *usernameFlag, *passwordFlag, *authmethodFlag, *protocolFlag)
		functions.GetClusterDetails(kafkaConnection)
	case "describetopic":
		kafkaConnection := functions.Connect(*clusterFlag, *usernameFlag, *passwordFlag, *authmethodFlag, *protocolFlag)
		functions.GetTopicDetails(kafkaConnection, *topicFlag)
	default:
		fmt.Printf("no valid command\n")
		os.Exit(9)
	}
}
