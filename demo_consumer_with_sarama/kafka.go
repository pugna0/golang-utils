package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

// initKafka, nonautomatic committing consumer
// return avalible broker for committing offset
func initKafka(servers []string) sarama.Consumer {

	config := sarama.NewConfig()
	templateOffset := struct {
		CommitInterval time.Duration
		Initial        int64
		Retention      time.Duration
	}{
		CommitInterval: 9999999,
		Initial:        sarama.OffsetOldest,
		Retention:      0,
	}
	if errConfig := config.Validate(); errConfig != nil {
		fmt.Println(errConfig)
		panic(errConfig)
	}
	config.Consumer.Offsets = templateOffset

	consumer, err := sarama.NewConsumer(servers, config)
	if err != nil {
		panic(err)
	}
	return consumer
}

func consumeMessage(consumer sarama.Consumer, topic string, wg *sync.WaitGroup,
	recieveCH chan *sarama.ConsumerMessage, consumerGroup string,
	avalibleBroker *sarama.Broker) (int64, error) {

	// recieveCH should be closed by channel reader.
	partitionList, err := consumer.Partitions(topic)
	partitionNumbers := len(partitionList)
	// recieveCH := make(chan *sarama.ConsumerMessage, partitionList)
	if err != nil {
		return 0, err
		// panic(err)
	} else {

		for _, partition := range partitionList {
			var partitionNumber int32 = partition
			// fetch partition's offset before creating PartitionConsumer
			offsetRequest := new(sarama.OffsetFetchRequest)
			offsetRequest.ConsumerGroup = consumerGroup
			offsetRequest.AddPartition(topic, partitionNumber)
			offsetFetchResponse, error := avalibleBroker.FetchOffset(offsetRequest)
			if error != nil {
				fmt.Println("fetching offset failed")
				panic(error)
			}
			offsetFetchResponseBlock := offsetFetchResponse.GetBlock(topic, partition)
			if offsetFetchResponseBlock.Err != 0 {
				fmt.Println("offset response error")
				// panic(offsetFetchResponseBlock.Err)
			}
			// next message
			offset := offsetFetchResponseBlock.Offset + 1
			partitionConsumer, err := consumer.ConsumePartition(topic, partition,
				offset)

			if err != nil {
				panic(err)
			}
			go func() {
				defer func() {
					// catch panicing for "writing to closed channel"
					if r := recover(); r != nil {
						// var ok bool
						// err, ok = r.(error)
						// if !ok {
						// 	errMsg := fmt.Errorf("Kafka Consumer Error: %s", err)
						// 	fmt.Println(errMsg)
						// }
						// TODO recover?

					}
					partitionConsumer.Close()
					wg.Done()
					fmt.Println("consumer routine exited")

				}()
			consumerLoopInRoutine:
				for {
					select {
					case msg, opened := <-partitionConsumer.Messages():
						if !opened {
							fmt.Println("consumer channel closed")
							break consumerLoopInRoutine
						}

						recieveCH <- msg

						fmt.Printf("Goroutine for consumer of partition %d is done.\n",
							partitionNumber)

					}
				}
			}()
			// add a goroutine
			wg.Add(1)
			fmt.Println("add one routine")
		}
	}
	return int64(partitionNumbers), nil
}

func findFirstAvalibleBroker(servers []string) *sarama.Broker {
	var avalibleBroker *sarama.Broker
	// defer avalibleBroker.Close()
	for _, b := range servers {
		tmpBroker := sarama.NewBroker(b)
		tmpBroker.Open(nil)
		if c, err := tmpBroker.Connected(); c {
			avalibleBroker = tmpBroker
			break
		} else {
			fmt.Printf("Broker %s is not avalibe, error msg: %s\n", b, err)
		}
	}
	if avalibleBroker == nil {
		fmt.Println("No avalible Broker in given parameters")
		return nil
	}
	return avalibleBroker
}

func commitOffset(broker *sarama.Broker, topic string, consumerGroup string,
	partition int32, offset int64) error {
	offsetCommitRequest := new(sarama.OffsetCommitRequest)
	offsetCommitRequest.ConsumerGroup = "pugna-group-123"
	offsetCommitRequest.Version = 0
	offsetCommitRequest.AddBlock(topic, partition, offset, time.Now().Unix(), "test-commiting")
	fmt.Printf("offset: %d on partition: %d will be commited\n", offset, partition)
	offsetCommitResponse, err_response := broker.CommitOffset(offsetCommitRequest)

	if err_response != nil {
		fmt.Println("offset committing failed")
		fmt.Println(offsetCommitResponse)
		return err_response
	}
	return nil
}
