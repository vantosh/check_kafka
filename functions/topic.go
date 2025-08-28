/*
 * check_kafka
 *
 * (c) Copyright 2024 VanTosh, all rights reserverd
 * Author : Toshaan Bharvani <toshaan@vantosh.com>
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

func GetTopicDetails(aK *kafka.AdminClient, topicName string) {
	topics := []string{topicName}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	describeTopicsResult, err := aK.DescribeTopics(ctx, kafka.NewTopicCollectionOfTopicNames(topics), kafka.SetAdminOptionIncludeAuthorizedOperations(false))
	if err != nil {
		fmt.Printf("Failed to describe topic %s\n%s", topicName, err)
		os.Exit(1)
	}

	fmt.Printf("Topic %s available with %d partitions\n", topicName, len(describeTopicsResult.TopicDescriptions[0].Partitions))
	for i := 0; i < len(describeTopicsResult.TopicDescriptions[0].Partitions); i++ {
		fmt.Printf("\tPartition %d with leader %s and replica count %d\n",
			describeTopicsResult.TopicDescriptions[0].Partitions[i].Partition, describeTopicsResult.TopicDescriptions[0].Partitions[i].Leader, len(describeTopicsResult.TopicDescriptions[0].Partitions[i].Replicas))
	}
	fmt.Printf("\n|partitions=%d;;;;", len(describeTopicsResult.TopicDescriptions[0].Partitions))
	os.Exit(0)
}

func GetTopicPartitions(aK *kafka.AdminClient, topicName string) []kafka.TopicPartition {
	topics := []string{topicName}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	describeTopicsResult, err := aK.DescribeTopics(ctx, kafka.NewTopicCollectionOfTopicNames(topics), kafka.SetAdminOptionIncludeAuthorizedOperations(false))
	if err != nil {
		fmt.Printf("Failed to describe topic %s\n%s", topicName, err)
		os.Exit(1)
	}

	var partitions []kafka.TopicPartition
	for i := 0; i < len(describeTopicsResult.TopicDescriptions[0].Partitions); i++ {
		partitions = append(partitions, kafka.TopicPartition{
			Topic: &topicName,
			Partition: int32(describeTopicsResult.TopicDescriptions[0].Partitions[i].Partition),
		})
	}

	return partitions
}
