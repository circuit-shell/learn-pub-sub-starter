package main

import (
	"fmt"
	"log"
	"time"

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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create Channel: %v", err)
	}

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
		handlerMove(gamestate, ch),
	)
	if err != nil {
		log.Fatalf("could not Subscribe to move queue: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gamestate, ch),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
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

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(dw gamelogic.RecognitionOfWar) pubsub.Actype {
	return func(dw gamelogic.RecognitionOfWar) pubsub.Actype {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(dw)

		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeYouWon:
			logMsg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(ch, logMsg, gs.GetUsername())
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeOpponentWon:
			logMsg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(ch, logMsg, gs.GetUsername())
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			logMsg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(ch, logMsg, gs.GetUsername())
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown war outcome")
		return pubsub.NackDiscard
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Actype {
	return func(move gamelogic.ArmyMove) pubsub.Actype {
		defer fmt.Print("> ")
		switch gs.HandleMove(move) {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				})
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func publishGameLog(ch *amqp.Channel, message, username string) error {
	key := routing.GameLogSlug + "." + username
	val := routing.GameLog{
		Message:     message,
		Username:    username,
		CurrentTime: time.Now(),
	}
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, key, val)
}
