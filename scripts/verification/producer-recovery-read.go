package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	marketv1 "financial-streaming-platform/gen/go/market/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func main() {
	topic := flag.String("topic", "", "Isolated topic to inspect")
	kind := flag.String("kind", "input", "input, ema, or advisory")
	count := flag.Int("count", 0, "Number of records captured from offset zero")
	output := flag.String("output", "", "JSON output path")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:19092"), kgo.ConsumePartitions(
		map[string]map[int32]kgo.Offset{*topic: {0: kgo.NewOffset().At(0)}},
	))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	records := []map[string]any{}
	for len(records) < *count {
		fetches := client.PollRecords(ctx, *count-len(records))
		if err := ctx.Err(); err != nil {
			panic(err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}
		fetches.EachRecord(func(record *kgo.Record) {
			value := map[string]any{}
			if *kind == "input" {
				event := new(marketv1.PriceEvent)
				if err := proto.Unmarshal(record.Value, event); err != nil {
					panic(err)
				}
				if string(record.Key) != event.GetSymbol() {
					panic("input key mismatch")
				}
				value["symbol"] = event.GetSymbol()
				value["row"] = event.GetSourceRow()
				value["time"] = event.GetTradeTime().AsTime().UnixMilli()
				value["price"] = strconv.FormatUint(event.GetLastPriceMicros(), 10)
			} else {
				var window *marketv1.EmaWindow
				if *kind == "ema" {
					window = new(marketv1.EmaWindow)
					if err := proto.Unmarshal(record.Value, window); err != nil {
						panic(err)
					}
				} else {
					advice := new(marketv1.TradeAdvisory)
					if err := proto.Unmarshal(record.Value, advice); err != nil {
						panic(err)
					}
					window = advice.GetEmaWindow()
					value["action"] = advice.GetAction().String()
					value["previousEma38"] = advice.GetPreviousEma38()
					value["previousEma100"] = advice.GetPreviousEma100()
				}
				if string(record.Key) != window.GetSymbol() {
					panic("output key mismatch")
				}
				value["symbol"] = window.GetSymbol()
				value["start"] = window.GetWindowStart().AsTime().UnixMilli()
				value["end"] = window.GetWindowEnd().AsTime().UnixMilli()
				value["price"] = strconv.FormatUint(window.GetClosingPriceMicros(), 10)
				value["ema38"] = window.GetEma38()
				value["ema100"] = window.GetEma100()
			}
			records = append(records, value)
		})
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(*output, data, 0600); err != nil {
		panic(err)
	}
}
