package topic

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Log struct {
	UserID    string
	AccountID string
	Amount    float64
	Status    string
}

type LogTopic interface {
	Start(ctx context.Context, log Log) error
}

type TransactionTopic struct {
	reader *kafka.Reader
}

func NewTransactionTopic(brokers []string, topic string, groupID string) *TransactionTopic {
	return &TransactionTopic{
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  brokers,
				Topic:    topic,
				MinBytes: 10e3, // 10KB
				MaxBytes: 10e6, // 10MB
			}),
	}
}

func (t *TransactionTopic) Start(ctx context.Context) error {
	for {
		msg, err := t.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Consumed log: %s\n", string(msg.Value))
		// 這邊可以加上處理log的邏輯，好比說將log寫入資料庫?或是進行其他處理
	}
}
