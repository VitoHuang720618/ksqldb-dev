package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

type Log struct {
	UserID    string  `json:"user_id"`
	AccountID string  `json:"account_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Timestamp string  `json:"timestamp"`
}

type LogTopic interface {
	Start(ctx context.Context, log Log) error
}

type TransactionTopic struct {
	writer *kafka.Writer
}

func NewTransactionTopic(brokers []string, topic string) *TransactionTopic {
	return &TransactionTopic{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (t *TransactionTopic) Start(log Log, ctx context.Context) error {
	msg, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("failed to marshal log message: %w", err)
	}
	m := kafka.Message{
		Key:   []byte(log.UserID),
		Value: msg,
	}
	return t.writer.WriteMessages(ctx, m)
}

// 底下為隨機產生transaction資料

type LogGenerator interface {
	GenLog() Log
}

type RandomLogGenerator struct {
	userIDs    []string
	accountIDs []string
	statuses   []string
}

func NewRandomLogGenerator(userIDs, accountIDs, statuses []string) *RandomLogGenerator {
	return &RandomLogGenerator{
		userIDs:    userIDs,
		accountIDs: accountIDs,
		statuses:   statuses,
	}
}

func (g *RandomLogGenerator) Generate() Log {
	return Log{
		UserID:    g.userIDs[rand.Intn(len(g.userIDs))],
		AccountID: g.accountIDs[rand.Intn(len(g.accountIDs))],
		Amount:    float64(rand.Intn(1000)),
		Status:    g.statuses[rand.Intn(len(g.statuses))],
		Timestamp: time.Now().Format(time.RFC3339),
	}
}
