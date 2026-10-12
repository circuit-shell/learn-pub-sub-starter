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
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(pubsub.RabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)

	}
	defer conn.Close()
	fmt.Println("RabbitMQ connection started.")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gamestate := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+userName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gamestate),
	)
	if err != nil {
		log.Fatalf("could not declare or bind pause queue: %v", err)
	}

	for {
		userInputs := gamelogic.GetInput()
		if len(userInputs) == 0 {
			continue
		}
		switch userInputs[0] {
		case "move":
			fmt.Println("Executing move command...")
			armyMove, err := gamestate.CommandMove(userInputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("Move command executed: ", armyMove)
		case "spawn":
			fmt.Println("Executing spawn command...")
			err := gamestate.CommandSpawn(userInputs)
			if err != nil {
				log.Println(err)
				continue
			}
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		case "clear":
			fmt.Print("\033[H\033[2J")
		default:
			fmt.Println("command ")
			gamelogic.PrintClientHelp()
		}
	}

}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
