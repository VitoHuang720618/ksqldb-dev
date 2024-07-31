package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thmeitz/ksqldb-go"
	"github.com/thmeitz/ksqldb-go/net"
)

const (
	ksqlDBServerURL  = "http://localhost:8088"
	telegramBotToken = "YOUR_TELEGRAM_BOT_TOKEN"
	telegramChatID   = "YOUR_TELEGRAM_CHAT_ID"
)

func main() {
	options := net.Options{
		BaseUrl:   ksqlDBServerURL,
		AllowHTTP: true,
	}
	kcl, err := ksqldb.NewClientWithOptions(options)
	if err != nil {
		log.Fatal("Failed to create ksqlDB client:", err)
	}
	defer kcl.Close()

	query := `SELECT * FROM failed_transactions_stream EMIT CHANGES;`
	rowChannel := make(chan ksqldb.Row)
	headerChannel := make(chan ksqldb.Header, 1)

	fmt.Println("Starting to listen for failed transactions...")
	go func() {
		for row := range rowChannel {
			if row != nil {
				if len(row) >= 5 {
					userID, _ := row[0].(string)
					accountID, _ := row[1].(string)
					amount, _ := row[2].(float64)
					status, _ := row[3].(string)
					timestamp, _ := row[4].(string)

					fmt.Printf("交易失敗！\n用戶ID: %v\n帳號ID: %v\n金額: %v\n狀態: %v\n時間: %v\n",
						userID, accountID, amount, status, timestamp)

					// telegram alert
					// sendTelegramAlert(fmt.Sprintf("交易失敗！用戶ID: %v, 帳號ID: %v, 金額: %v, 狀態: %v, 時間: %v",
					//	userID, accountID, amount, status, timestamp))
				}
			}
		}
	}()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		err = kcl.Push(ctx, ksqldb.QueryOptions{Sql: query}, rowChannel, headerChannel)
		if err != nil {
			log.Fatal("Failed to push query:", err)
		}
		cancel()

		// wait ...
		time.Sleep(10 * time.Second)
	}
}

func sendTelegramAlert(message string) {
	//url := "https://api.telegram.org/bot" + telegramBotToken + "/sendMessage"
	//payload := map[string]string{
	//	"chat_id": telegramChatID,
	//	"text":    message,
	//}

	//payloadBytes, _ := json.Marshal(payload)
	//resp, err := http.Post(url, "application/json", bytes.NewBuffer(payloadBytes))
	//if err != nil {
	//	log.Printf("Failed to send Telegram alert: %v", err)
	//	return
	//}
	//defer resp.Body.Close()

	//if resp.StatusCode != http.StatusOK {
	//	log.Printf("Failed to send Telegram alert. Status: %s", resp.Status)
	//	return
	//}

	// log.Println("Sent Telegram alert:", message)
}
