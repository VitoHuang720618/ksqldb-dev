package main

import (
	"context"
	"fmt"
	"time"

	"ksqldb/producer/internal/topic"
)

func main() {
	p := topic.NewTransactionTopic([]string{"localhost:29092"}, "transaction-logs")
	g := topic.NewRandomLogGenerator(
		[]string{
			"user001", "user002", "user003", "user004", "user005", "user006", "user007", "user008", "user009", "user010",
			"user011", "user012", "user013", "user014", "user015", "user016", "user017", "user018", "user019", "user020",
			"user021", "user022", "user023", "user024", "user025", "user026", "user027", "user028", "user029", "user030",
			"user031", "user032", "user033", "user034", "user035", "user036", "user037", "user038", "user039", "user040",
			"user041", "user042", "user043", "user044", "user045", "user046", "user047", "user048", "user049", "user050",
			"user051", "user052", "user053", "user054", "user055", "user056", "user057", "user058", "user059", "user060",
			"user061", "user062", "user063", "user064", "user065", "user066", "user067", "user068", "user069", "user070",
			"user071", "user072", "user073", "user074", "user075", "user076", "user077", "user078", "user079", "user080",
			"user081", "user082", "user083", "user084", "user085", "user086", "user087", "user088", "user089", "user090",
			"user091", "user092", "user093", "user094", "user095", "user096", "user097", "user098", "user099", "user100",
		},
		[]string{
			"account001", "account002", "account003", "account004", "account005", "account006", "account007", "account008", "account009", "account010",
			"account011", "account012", "account013", "account014", "account015", "account016", "account017", "account018", "account019", "account020",
			"account021", "account022", "account023", "account024", "account025", "account026", "account027", "account028", "account029", "account030",
			"account031", "account032", "account033", "account034", "account035", "account036", "account037", "account038", "account039", "account040",
			"account041", "account042", "account043", "account044", "account045", "account046", "account047", "account048", "account049", "account050",
			"account051", "account052", "account053", "account054", "account055", "account056", "account057", "account058", "account059", "account060",
			"account061", "account062", "account063", "account064", "account065", "account066", "account067", "account068", "account069", "account070",
			"account071", "account072", "account073", "account074", "account075", "account076", "account077", "account078", "account079", "account080",
			"account081", "account082", "account083", "account084", "account085", "account086", "account087", "account088", "account089", "account090",
			"account091", "account092", "account093", "account094", "account095", "account096", "account097", "account098", "account099", "account100",
		},
		[]string{"SUCCESS", "FAILURE"},
	)
	fmt.Println("Start producing logs...")
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for i := 0; i < 10; i++ {
				// 隨機產生log
				log := g.Generate()

				// 將log傳入producer
				err := p.Start(log, context.Background())

				// error handling
				if err != nil {
					fmt.Printf("failed to produce message: %v", err)
				} else {
					fmt.Printf("Produced log:\n  UserID: %s\n  AccountID: %s\n  Amount: %.2f\n  Status: %s\n", log.UserID, log.AccountID, log.Amount, log.Status)
				}
			}
		}
	}
}
