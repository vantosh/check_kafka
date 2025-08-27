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

func GetConsumerGroupLag(aK *kafka.AdminClient, consumerGroupName string, topicName string, warningLevel int64, criticalLevel int64, verboseBool bool) {
	partitions := GetTopicPartitions(aK, topicName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	gps := []kafka.ConsumerGroupTopicPartitions{
		{
			Group: consumerGroupName,
			Partitions: partitions,
		},
	}
	cgO, cgErr := aK.ListConsumerGroupOffsets(ctx, gps, kafka.SetAdminRequireStableOffsets(true))
	if cgErr != nil {
		fmt.Printf("Failed to list consumer group %s on topic %s offsets\n%s\n", consumerGroupName, topicName, cgErr)
		os.Exit(2)
	}

	var topicLags int64 = 0
	var debugLines string
	for partKey, partValue := range partitions {
		topicPartitionOffsets := make(map[kafka.TopicPartition]kafka.OffsetSpec)
		tp := kafka.TopicPartition{
			Topic: &topicName,
			Partition: int32(partitions[partKey].Partition),
		}
		topicPartitionOffsets[tp] = kafka.EarliestOffsetSpec

		lO, lErr := aK.ListOffsets(ctx, topicPartitionOffsets, kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
		if lErr != nil {
			fmt.Printf("Failed to list topic %s offsets\n%s\n", lErr)
			os.Exit(2)
		}
		topicOffset := int64(lO.ResultInfos[tp].Offset)

		consumerGroupOffset := int64(cgO.ConsumerGroupsTopicPartitions[0].Partitions[partKey].Offset)

		topicLag := topicOffset - consumerGroupOffset
		debugLines = debugLines + fmt.Sprintf("\t\tPartition %v (%d) : %d (%d - %d)\n", partValue, partKey, topicLag, topicOffset, consumerGroupOffset)
		topicLags = topicLags + topicLag
	}

	fmt.Printf("Consumer Group %s on topic %s has a Lag of %d \n", consumerGroupName, topicName, topicLags)
	if(verboseBool == true) {
		fmt.Printf(debugLines)
	}
	fmt.Printf("|lag=%d;%d;%d;0;999999;", topicLags, warningLevel, criticalLevel)
	if(topicLags > warningLevel) {
		os.Exit(1)
	} else if(topicLags > criticalLevel) {
		os.Exit(2)
	} else {
		os.Exit(0)
	}


}
