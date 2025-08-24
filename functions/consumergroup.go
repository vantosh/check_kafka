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

package functions

import (
	"context"
	"fmt"
	"os"
	"time"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func GetConsumerGroups(aK *kafka.AdminClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	var options []kafka.ListConsumerGroupsAdminOption
	listConsumerGroups, err := aK.ListConsumerGroups(ctx, options...)
	if err != nil {
		fmt.Printf("Failed to describe consumer groups\n%s", err)
		os.Exit(2)
	}

	cGroups := listConsumerGroups.Valid
	fmt.Printf("Consumer Groups : %d\n", len(cGroups))
	for _, cGroup := range cGroups {
		fmt.Printf("\tID: %s - state: %s - type: %s - simple: %v\n", cGroup.GroupID, cGroup.State, cGroup.Type, cGroup.IsSimpleConsumerGroup)
	}
	fmt.Printf("\n|groups=%d;;;;", len(cGroups))
	os.Exit(0)
}

func GetConsumerGroupDetails(aK *kafka.AdminClient, consumerGroupName string) {
	consumergroups := []string{consumerGroupName}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	describeGroupsResult, err := aK.DescribeConsumerGroups(ctx, consumergroups, kafka.SetAdminOptionIncludeAuthorizedOperations(false))
	if err != nil {
		fmt.Printf("Failed to describe group: %s\n%s", consumerGroupName, err)
		os.Exit(2)
	}

	if(describeGroupsResult.ConsumerGroupDescriptions[0].Stat == "Dead") {
		fmt.Printf("Consumer Group %s\n\tState: %s\n\tCoordinator: %s\n\tError: %s",
			consumerGroupName,
			describeGroupsResult.ConsumerGroupDescriptions[0].State,
			describeGroupsResult.ConsumerGroupDescriptions[0].Coordinator,
			describeGroupsResult.ConsumerGroupDescriptions[0].Error)
		os.Exit(1)
	}

	fmt.Printf("Consumer Group %s\n\tState: %s\n\tCoordinator: %s",
		consumerGroupName,
		describeGroupsResult.ConsumerGroupDescriptions[0].State,
		describeGroupsResult.ConsumerGroupDescriptions[0].Coordinator)
	os.Exit(0)
}
