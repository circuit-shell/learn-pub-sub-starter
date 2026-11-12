package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const RabbitConnString = "amqp://guest:guest@localhost:5672/"

var empmtyArgsTable amqp.Table

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type Actype string

const (
	Ack         = "Ack"
	NackRequeue = "NackRequeue"
	NackDiscard = "NackDiscard"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	// 1. Create Channel
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel: %w", err)
	}

	// 2. Set queue options
	durable := queueType == Durable
	autodelete := queueType == Transient
	exclusive := queueType == Transient
	nowait := false

	table := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	// 3. Declare the queue
	queue, err := ch.QueueDeclare(queueName, durable, autodelete, exclusive, nowait, table)
	if err != nil {
		ch.Close() // Clean up the channel if declaration fails
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue '%s': %w", queueName, err)
	}

	// 4. Bind the queue

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue '%s': %w", err)
	}

	return ch, queue, nil
}
