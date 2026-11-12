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
	handler func(T) Actype,
) error {

	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Could not declare and bind queue: %v", err)
	}

	chDelivery, err := ch.Consume(queue.Name, "", false, false, false, false, empmtyArgsTable)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for msg := range chDelivery {
			var body T
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			switch handler(body) {
			case Ack:
				msg.Ack(false)
			case NackDiscard:
				msg.Nack(false, false)
			case NackRequeue:
				msg.Nack(false, true)
			}
		}
	}()

	return nil
}
