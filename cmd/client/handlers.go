package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerDeps struct {
	GS *gamelogic.GameState
}

type HandlerDepsWithChannel struct {
	Ch *amqp.Channel
	GS *gamelogic.GameState
}

func handlerPause(d HandlerDeps) func(routing.PlayingState) pubsub.Acktype {
	gs := d.GS
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(d HandlerDepsWithChannel) func(gamelogic.ArmyMove) pubsub.Acktype {
	pubCh, gs := d.Ch, d.GS
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
				pubCh,
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

func handlerWar(d HandlerDepsWithChannel) func(dw gamelogic.RecognitionOfWar) pubsub.Acktype {
	pubCh, gs := d.Ch, d.GS
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

		if err := pubsub.PublishGob(
			pubCh,
			routing.ExchangePerilTopic,
			queueGameLogKey(gs.Player.Username),
			log,
		); err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
