package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, qu, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind new channel: %w", err)
	}
	deliveryCh, err := ch.Consume(qu.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not create new channel for delivery: %w", err)
	}
	// Start a goroutine that ranges over the channel of deliveries, and for each message:
	go func() {
		for d := range deliveryCh {
			var msg T
			json.Unmarshal(d.Body, &msg)
			handler(msg)
			d.Ack(false)
		}
	}()
	return nil
}
