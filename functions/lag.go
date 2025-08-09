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

func GetConsumerGroupLag(aK *kafka.AdminClient, consumerGroupName string, topicName string, warningLevel int64, criticalLevel int64) {
	partitions := GetTopicPartitions(aK, topicName)

	gps := []kafka.ConsumerGroupTopicPartitions{
		{
			Group: consumerGroupName,
			Partitions: partitions,
		},
	}

	topicPartitionOffsets := make(map[kafka.TopicPartition]kafka.OffsetSpec)
	tp := kafka.TopicPartition{
		Topic: &topicName,
		Partition: int32(partitions[0].Partition),
	}
	//topicPartitionOffsets[tp] = kafka.LatestOffsetSpec
	topicPartitionOffsets[tp] = kafka.EarliestOffsetSpec

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	lO, lErr := aK.ListOffsets(ctx, topicPartitionOffsets, kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
	if lErr != nil {
		fmt.Printf("Failed to list topic %s offsets\n%s\n", lErr)
		os.Exit(1)
	}
	topicLag := int64(lO.ResultInfos[tp].Offset)

	cgO, cgErr := aK.ListConsumerGroupOffsets(ctx, gps, kafka.SetAdminRequireStableOffsets(true))
	if cgErr != nil {
		fmt.Printf("Failed to list consumer group %s on topic %s offsets\n%s\n", consumerGroupName, topicName, cgErr)
		os.Exit(1)
	}
	consumerGroupOffset := cgO.ConsumerGroupsTopicPartitions[0].Partitions[0].Offset

	fmt.Printf("Consumer Group %s on topic %s has a Lag of %s (offset %s)\n", consumerGroupName, topicName, topicLag, consumerGroupOffset)
	fmt.Printf("|lag=%d;%d;%d;0;999999;", topicLag, warningLevel, criticalLevel)
	if(topicLag > warningLevel) {
		os.Exit(1)
	} else if(topicLag > criticalLevel) {
		os.Exit(2)
	} else {
		os.Exit(0)
	}


}
