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
	//"context"
	"fmt"
	"os"
	"time"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func GetTopicLastActivity(aK *kafka.AdminClient, cK *kafka.Consumer, consumerGroupName string, topicName string, warningLevel int64, criticalLevel int64, verboseBool bool) {
	partitions := GetTopicPartitions(aK, topicName)

	nowTimestamp := time.Now()
	var latestTimestamp time.Time

	for _, p := range partitions {
		tp := kafka.TopicPartition{
			Topic: &topicName,
			Partition: p.Partition,
		}

		lowWM, highWM, errWM := cK.QueryWatermarkOffsets(topicName, p.Partition, 1000)
		if(errWM != nil) {
			fmt.Printf("Error getting watermark offsets for topic %s partition %d\n\tWatermarks : %d - %d\nERROR: %s\n", topicName, p.Partition, lowWM, highWM, errWM)
			os.Exit(3)
		}

		if highWM == 0 {
			fmt.Printf("There is no high watermark for topic %s partition %d\n", topicName, p.Partition)
			os.Exit(3)
		}

		tp.Offset = kafka.Offset(highWM - 1)
		errTP := cK.Assign([]kafka.TopicPartition{tp})
		if errTP != nil {
			fmt.Printf("Failed to assign topic %s partition %d\nERROR: %s\n", topicName, p.Partition, errTP)
			os.Exit(3)
		}

		lastMSG, errMSG := cK.ReadMessage(5 * time.Second)
		if errMSG != nil {
			fmt.Printf("Error reading last message from topic %s partition %d\nERROR: %s\n", topicName, p.Partition, errMSG)
			os.Exit(2)
		}

		if lastMSG.Timestamp.After(latestTimestamp) {
			latestTimestamp = lastMSG.Timestamp
		}
	}

	timestampDiff := nowTimestamp.Unix() - latestTimestamp.Unix()

	fmt.Printf("Latest activity for topic %s at %s\n", topicName, latestTimestamp.Format("Monday 02 January 2006 15:04:05"))
	fmt.Printf("|diff=%d;%d;%d;0;99999;", timestampDiff, warningLevel, criticalLevel)
	if(timestampDiff > criticalLevel) {
		os.Exit(2)
	} else if(timestampDiff > warningLevel) {
		os.Exit(1)
	} else {
		os.Exit(0)
	}


}
