package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("could not marshall to JSON: %v", err)
	}
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBytes,
		},
	)
}

type simpleQueueType int

const (
	Durable   simpleQueueType = iota // 0
	Transient                        // 1
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType simpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create new channel: %w", err)
	}
	isDurable := false
	isAutoDelete := false
	isExclusive := false
	isNoWait := false
	var args amqp.Table = nil
	switch queueType {
	case Durable:
		isDurable = true
	case Transient:
		isAutoDelete = true
		isExclusive = true
	default:
		return nil, amqp.Queue{}, fmt.Errorf("unknown queueType: %v", queueType)
	}

	qu, err := ch.QueueDeclare(
		queueName,
		isDurable,
		isAutoDelete,
		isExclusive,
		isNoWait,
		args,
	)
	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %w", err)
	}

	err = ch.QueueBind(
		queueName,
		key,
		exchange,
		isNoWait,
		args,
	)
	if err != nil {
		ch.Close()
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %w", err)
	}
	return ch, qu, nil
}
