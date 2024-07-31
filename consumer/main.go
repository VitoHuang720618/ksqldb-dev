package main

import (
	"context"
	"fmt"

	"ksqldb/consumer/internal/topic"
)

func main() {
	b := []string{"localhost:29092"}
	t := "transaction-logs"
	gId := "log-consumer-group"

	c := topic.NewTransactionTopic(b, t, gId)
	err := c.Start(context.Background())
	if err != nil {
		fmt.Printf("failed to consume message: %v", err)
	}
}
