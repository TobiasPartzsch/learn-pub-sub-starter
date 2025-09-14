package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create new channel: %w", err)
	}
	isDurable := false
	isAutoDelete := false
	isExclusive := false
	isNoWait := false
	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
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
