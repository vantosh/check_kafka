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
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"time"
)

func GetClusterDetails(aK *kafka.AdminClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	clusterDesc, err := aK.DescribeCluster(ctx, kafka.SetAdminOptionIncludeAuthorizedOperations(true))
	if err != nil {
		fmt.Printf("Failed to describe cluster\n%s", err)
		os.Exit(1)
	}

	fmt.Printf("Cluster with ID %s using Controller %s with nodes : %s", *clusterDesc.ClusterID, clusterDesc.Controller, clusterDesc.Nodes)
	os.Exit(0)
}
