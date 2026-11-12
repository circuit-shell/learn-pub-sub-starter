package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

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

