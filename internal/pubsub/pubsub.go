package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const RabbitConnString = "amqp://guest:guest@localhost:5672/"

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		log.Fatalf("Error occurred marshalling in PublishJSON: %s", err)
	}
	ctx := context.Background()
	mandatory := false
	immediate := false
	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonData,
	}
	ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	return nil
}

// Declare and Bind

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {

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
	var args amqp.Table

	// 3. Declare the queue
	queue, err := ch.QueueDeclare(queueName, durable, autodelete, exclusive, nowait, args)
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
