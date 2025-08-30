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
	//"time"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func GetTopicLastActivity(aK *kafka.AdminClient, cK *kafka.Consumer, consumerGroupName string, topicName string, warningLevel int64, criticalLevel int64, verboseBool bool) {
	partitions := GetTopicPartitions(aK, topicName)

	//ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	//defer cancel()

	offsetsInfo, offsetError := cK.OffsetsForTimes(partitions, 90000)
	if(offsetError != nil) {
		fmt.Printf("Offset Error : %s", offsetError)
		os.Exit(2)
	}

	var topicTimestampDiff int64 = 0
	var debugLines string
	for _, partValue := range partitions {
		offsetTimestamp := int64(offsetsInfo[partValue.Partition].Offset)
		debugLines = debugLines + fmt.Sprintf("\tPartition %d : %d\n", partValue.Partition, offsetTimestamp)
		topicTimestampDiff = topicTimestampDiff + offsetTimestamp
	}

	fmt.Printf("Topic %s has had activity after %d\n", topicName, topicTimestampDiff)
	if(verboseBool == true) {
		fmt.Printf(debugLines)
	}
	//fmt.Printf("|lag=%d;%d;%d;0;999999;", topicLags, warningLevel, criticalLevel)
	//if(topicLags > criticalLevel) {
	//	os.Exit(2)
	//} else if(topicLags > warningLevel) {
	//	os.Exit(1)
	//} else {
	//	os.Exit(0)
	//}
}
