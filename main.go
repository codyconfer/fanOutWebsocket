package main

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/reactivex/rxgo/v2"
)

type Message struct {
	Type    int
	Payload []byte
}

const ChannelName = "channel"

func wireSubscribe(rdb *redis.Client) rxgo.Observable {
	subscribeOperator := make(chan rxgo.Item)
	subscribeObservable := rxgo.FromChannel(subscribeOperator)
	go func() {
		pubsub := rdb.Subscribe(context.Background(), ChannelName)
		defer func() {
			if err := pubsub.Close(); err != nil {
				log.Printf("error closing pubsub: %v", err)
			}
		}()

		for msg := range pubsub.Channel() {
			subscribeOperator <- rxgo.Of(Message{Type: 1, Payload: []byte(msg.Payload)})
		}
	}()
	subscribeObservable.Connect(context.Background())
	return subscribeObservable
}

func wirePublish(rdb *redis.Client) chan<- rxgo.Item {
	publishOperator := make(chan rxgo.Item)
	publishObservable := rxgo.FromChannel(publishOperator)
	publishObservable.DoOnNext(func(item interface{}) {
		message, ok := item.(Message)
		if !ok {
			log.Printf("invalid message type from client subject: %T", item)
			return
		}
		if err := Publish(rdb, ChannelName, message.Payload); err != nil {
			log.Printf("failed to publish message: %v", err)
		}
	})
	publishObservable.Connect(context.Background())

	return publishOperator
}

func main() {
	rdb, err := NewRedisClient()
	if err != nil {
		log.Fatalf("failed to create redis client: %v", err)
	}
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Printf("failed to close redis client: %v", err)
		}
	}()
	log.Println("Successfully connected to Redis!")
	operator := wirePublish(rdb)
	observable := wireSubscribe(rdb)
	httpRouter := NewRouter(operator, observable)
	if err := httpRouter.Run(); err != nil {
		log.Fatalf("Failed to run http router: %v", err)
	}
}
