package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)


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



func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	encodedData, err := encode(val)
	if err != nil {
		log.Fatalf("Error occurred marshalling in PublishJSON: %s", err)
	}
	ctx := context.Background()
	mandatory := false
	immediate := false
	msg := amqp.Publishing{
			ContentType: "application/gob",
		Body: encodedData,
	}
	ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	return nil
}


func encode[T any](val T) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}



