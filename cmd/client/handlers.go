package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")

		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
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

func handlerWar(ch *amqp.Channel, gs *gamelogic.GameState) func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(dw)

		var msg string
		switch warOutcome {
		case gamelogic.WarOutcomeYouWon:
			fallthrough
		case gamelogic.WarOutcomeOpponentWon:
			msg = fmt.Sprintf(gamelogic.WarWinFormat, winner, loser)
		case gamelogic.WarOutcomeDraw:
			msg = fmt.Sprintf(gamelogic.WarDrawFormat, winner, loser)
		default:
			// TODO: might want to differentiate between unknown result and uninteresting ones
			return pubsub.NackDiscard
		}
		log := routing.GameLog{
			Username:    gs.Player.Username,
			Message:     msg,
			CurrentTime: time.Now(),
		}

		if err := pubsub.PublishGob(ch,
			routing.ExchangePerilTopic,
			queueGameLogKey(gs.Player.Username),
			log,
		); err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
