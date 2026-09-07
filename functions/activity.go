/*
 * check_kafka
 *
 * (c) Copyright 2024, 2026 VanTosh, all rights reserverd
 * Author : Toshaan Bharvani <toshaan@vantosh.com>
 *
 * Use of this source code is governed by a Apache 2.0 license,
 * that can be found in the LICENSE file.
 *
 */

package functions

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"time"
)

func GetTopicLastActivity(aK *kafka.AdminClient, cK *kafka.Consumer, consumerGroupName string, topicName string, warningLevel int64, criticalLevel int64, verboseBool bool) {
	partitions := GetTopicPartitions(aK, topicName)

	nowTimestamp := time.Now()
	var latestTimestamp time.Time

	for _, p := range partitions {
		tp := kafka.TopicPartition{
			Topic:     &topicName,
			Partition: p.Partition,
		}

		lowWM, highWM, errWM := cK.QueryWatermarkOffsets(topicName, p.Partition, 1000)
		if errWM != nil {
			if verboseBool == true {
				fmt.Printf("Error getting watermark offsets for topic %s partition %d\n\tWatermarks : %d - %d\nERROR: %s\n", topicName, p.Partition, lowWM, highWM, errWM)
			}
		}

		if highWM == 0 {
			if verboseBool == true {
				fmt.Printf("There is no high watermark for topic %s partition %d\n", topicName, p.Partition)
			}
		} else if highWM == lowWM {
			if verboseBool == true {
				fmt.Printf("Low (%d) and High (%d) Watermark are equal on partition %d", lowWM, highWM, p.Partition)
			}
		} else {
			tp.Offset = kafka.Offset(highWM - 1)
			errTP := cK.Assign([]kafka.TopicPartition{tp})
			if errTP != nil {
				if verboseBool == true {
					fmt.Printf("Failed to assign topic %s partition %d\nERROR: %s\n", topicName, p.Partition, errTP)
				}
			}

			lastMSG, errMSG := cK.ReadMessage(5 * time.Second)
			if errMSG != nil {
				if verboseBool == true {
					fmt.Printf("Error reading last message from topic %s partition %d\nERROR: %s\n", topicName, p.Partition, errMSG)
				}
			}

			if lastMSG.Timestamp.After(latestTimestamp) {
				latestTimestamp = lastMSG.Timestamp
			}
		}
	}

	timestampDiff := nowTimestamp.Unix() - latestTimestamp.Unix()

	var preMsg string
	var exitCode int = 3
	if timestampDiff > criticalLevel {
		preMsg = "[CRITICAL] CRITICAL"
		exitCode = 2
	} else if timestampDiff > warningLevel {
		preMsg = "[WARNING] WARNiNG"
		exitCode = 1
	} else {
		preMsg = "[OK] OK"
		exitCode = 0
	}

	fmt.Printf("%s Latest activity of topic %s on %s\n", preMsg, topicName, latestTimestamp.Format("Monday 02 January 2006 15:04:05"))
	fmt.Printf("|diff=%d;%d;%d;0;99999;", timestampDiff, warningLevel, criticalLevel)
	os.Exit(exitCode)
}
