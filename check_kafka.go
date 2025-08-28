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
	consumerFlag := flag.String("consumergroup", "", "Kafka Consumer Group")
	warningFlag := flag.Int64("warning", 5000, "Warning Level")
	criticalFlag := flag.Int64("critical", 10000, "Critial Level")
	verboseFlag := flag.Bool("verbose", false, "Verbose output")
	flag.Parse()

	if(*commandFlag == "version") {
		version.PrintVersion()
		os.Exit(0)
	} else {
		kafkaAdmin := functions.AdminConnect(*clusterFlag, *usernameFlag, *passwordFlag, *authmethodFlag, *protocolFlag)
		switch *commandFlag {
		case "describecluster":
			functions.GetClusterDetails(kafkaAdmin)
		case "describetopic":
			functions.GetTopicDetails(kafkaAdmin, *topicFlag)
		case "listconsumergroups":
			functions.GetConsumerGroups(kafkaAdmin)
		case "describeconsumergroup":
			functions.GetConsumerGroupDetails(kafkaAdmin, *consumerFlag)
		case "consumergrouptopiclag":
			kafkaConsumer := functions.ConsumerConnect(*clusterFlag, *usernameFlag, *passwordFlag, *authmethodFlag, *protocolFlag, *consumerFlag)
			functions.GetConsumerGroupLag(kafkaAdmin, kafkaConsumer, *consumerFlag, *topicFlag, *warningFlag, *criticalFlag, *verboseFlag)
		default:
			fmt.Printf("no valid command\n")
			os.Exit(9)
		}
	}
}
