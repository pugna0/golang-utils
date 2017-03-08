package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup

	consumer := initKafka([]string{"10.6.8.223:9092"})
	avalibleBroker := findFirstAvalibleBroker([]string{"10.6.8.223:9092"})
	if consumer == nil || avalibleBroker == nil {
		fmt.Println("initialization Kafka failed")
		return
	}
	defer consumer.Close()

	ch := make(chan *sarama.ConsumerMessage)
	terminateCH := make(chan int64)
	_, err := consumeMessage(consumer, "go-kafka-123", &wg, ch, "pugna-group-123",
		avalibleBroker)
	if err != nil {
		fmt.Println("errors occur while consuming message from kafka")
		fmt.Println(err)
		return
	}
	fmt.Println("before consumerLoop")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
ConsumerLoop:
	for {
		select {
		case msg, opened := <-ch:
			if !opened {
				break ConsumerLoop
			}
			fmt.Printf("Consumed message offset %d\n", msg.Offset)
			fmt.Printf("Msg Content: %s\n", msg.Value)

			// now  commit offset
			offsetCommitRequest := new(sarama.OffsetCommitRequest)
			offsetCommitRequest.ConsumerGroup = "pugna-group-123"
			offsetCommitRequest.Version = 0
			offsetCommitRequest.AddBlock("go-kafka-123", msg.Partition, msg.Offset, time.Now().Unix(), "test-commiting")
			fmt.Printf("offset: %d will be commited\n", msg.Offset)
			offsetCommitResponse, err_response := avalibleBroker.CommitOffset(offsetCommitRequest)
			if err_response != nil {
				fmt.Println(err_response, offsetCommitResponse)
			}

		case sig := <-signals:
			fmt.Printf("Receive signal: %v\n", sig)
			close(terminateCH)
			close(ch)
		}
	}
	fmt.Println("consumerLoop done")

	wg.Wait()

	return
}
