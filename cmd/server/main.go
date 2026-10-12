package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril Server...")
	conn, err := amqp.Dial(pubsub.RabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Peril game server connected to RabbitMQ.")
	gamelogic.PrintServerHelp()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("could not connect Channel")
	}

	for {
		userInputs := gamelogic.GetInput()
		if len(userInputs) == 0 {
			continue
		}

		switch userInputs[0] {
		case "pause":
			fmt.Println("Pausing game...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				})
			if err != nil {
				log.Fatalf("could not publish to pauseKey: %v", err)
			}
		case "resume":
			fmt.Println("Resuming game...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				})
			if err != nil {
				log.Fatalf("could not publish to pauseKey: %v", err)
			}
		case "quit":
			fmt.Println("Terminating Peril server...")
			fmt.Println("RabbitMQ connection closed...")
			return
		case "help":
			gamelogic.PrintServerHelp()
		case "clear":
			fmt.Print("\033[H\033[2J")
		default:
			fmt.Println("Could not execute Unknown command ")
			gamelogic.PrintServerHelp()
		}
	}

	// ch, err := conn.Channel()
	// if err != nil {
	// 	log.Fatal("could not connect Channel")
	// }
	//
	// err = pubsub.PublishJSON(
	// 	ch,
	// 	routing.ExchangePerilDirect,
	// 	routing.PauseKey,
	// 	routing.PlayingState{
	// 		IsPaused: true,
	// 	})
	//
	// if err != nil {
	// 	log.Fatalf("could not publish pauseKey: %v", err)
	// }
	//
	// // wait for close signal
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	// fmt.Println("RabbitMQ connection closed.")
}
