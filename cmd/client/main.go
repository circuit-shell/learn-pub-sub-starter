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
		log.Fatalf("could not Subscribe to pause queue: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,           // exchange
		routing.ArmyMovesPrefix+"."+userName, // queueName
		routing.ArmyMovesPrefix+".*",         // key
		pubsub.Transient,
		handlerMove(gamestate),
	)
	if err != nil {
		log.Fatalf("could not Subscribe to move queue: %v", err)
	}

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
		case "move":
			fmt.Println("Executing move command...")
			armyMove, err := gamestate.CommandMove(userInputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+".*", // key
				armyMove,
			)
			if err != nil {
				log.Println("could not publish to pauseKey: %v", err)
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Actype {
	return func(ps routing.PlayingState) pubsub.Actype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.Actype {
	return func(move gamelogic.ArmyMove) pubsub.Actype {
		defer fmt.Print("> ")
		switch gs.HandleMove(move) {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		}
		return pubsub.NackDiscard
	}
}
