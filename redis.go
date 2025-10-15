package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/reactivex/rxgo/v2"
)

var ctx = context.Background()

func NewRedisClient() (*redis.Client, error) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return rdb, nil
}

func Subscribe(rdb *redis.Client, channelName string) rxgo.Observable {
	return rxgo.Create([]rxgo.Producer{func(ctx context.Context, next chan<- rxgo.Item) {
		pubsub := rdb.Subscribe(ctx, channelName)
		defer func(pubsub *redis.PubSub) {
			err := pubsub.Close()
			if err != nil {
				log.Printf("error closing subscription: %v", err)
			}
		}(pubsub)

		for msg := range pubsub.Channel() {
			next <- rxgo.Of(Message{Type: 1, Payload: []byte(msg.Payload)})
		}
	}})
}

func Publish(rdb *redis.Client, channelName string, message []byte) error {
	return rdb.Publish(ctx, channelName, message).Err()
}
