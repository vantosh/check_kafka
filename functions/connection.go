package functions

import (
	"fmt"
	"os"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"check_kafka/version"
)

func AdminConnect(clusterName string, userName string, passWord string, authMethod string, securityProtocol string) *kafka.AdminClient {
	var err error
	var a *kafka.AdminClient
	if(userName != "" && passWord != "") {
		a, err = kafka.NewAdminClient(&kafka.ConfigMap{
			"bootstrap.servers": clusterName,
			"sasl.username": userName,
			"sasl.password": passWord,
			"sasl.mechanism": authMethod,
			"security.protocol": securityProtocol,
			"client.id": fmt.Sprintf("%s - %s", version.Client, version.Version),
		})
	} else {
		a, err = kafka.NewAdminClient(&kafka.ConfigMap{
			"bootstrap.servers": clusterName,
			"client.id": fmt.Sprintf("%s - %s", version.Client, version.Version),
		})
	}
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(2)
	}
	return a
}


func ConsumerConnect(clusterName string, userName string, passWord string, authMethod string, securityProtocol string, consumerGroup string) *kafka.Consumer {
	var err error
	var c *kafka.Consumer
	if(userName != "" && passWord != "") {
		c, err = kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": clusterName,
			"sasl.username": userName,
			"sasl.password": passWord,
			"sasl.mechanism": authMethod,
			"security.protocol": securityProtocol,
			"client.id": fmt.Sprintf("%s - %s", version.Client, version.Version),
			"group.id": consumerGroup,
			"auto.offset.reset": "earliest",
		})
	} else {
		c, err = kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": clusterName,
			"client.id": fmt.Sprintf("%s - %s", version.Client, version.Version),
			"auto.offset.reset": "earliest",
		})
	}
	if err != nil {
		fmt.Printf("Failed to create Consumer client: %s\n", err)
		os.Exit(2)
	}
	return c
}
