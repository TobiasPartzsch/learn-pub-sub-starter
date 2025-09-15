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
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not open publish channel: %v", err)
	}
	defer pubCh.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not welcome the user: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		keyPause(username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(HandlerDeps{
			GS: gs,
		}),
	)
	if err != nil {
		log.Fatalf("could not subscribe pause handler: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		keyArmyMoves(username),
		bindArmyMovesPattern(),
		pubsub.Transient,
		handlerMove(HandlerDepsWithChannel{
			Ch: pubCh,
			GS: gs,
		}),
	)
	if err != nil {
		log.Fatalf("could not subscribe to movements: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(HandlerDepsWithChannel{
			Ch: pubCh,
			GS: gs,
		}),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if err := pubsub.PublishJSON(
				pubCh,
				routing.ExchangePerilTopic,
				keyArmyMoves(gs.Player.Username), // army_moves.username
				mv,
			); err != nil {
				log.Printf("failed to publish move: %v", err)
			} else {
				log.Printf(
					"published move to %q: units=%v -> %s",
					keyArmyMoves(gs.Player.Username),
					mv.Units,
					mv.ToLocation)
			}
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			// TODO: publish n malicious logs
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}

func key(prefix, user string) string { return prefix + "." + user }
func bindAll(prefix string) string   { return prefix + ".*" }

// Concrete helpers (optional thin wrappers)
func keyPause(user string) string           { return key(routing.PauseKey, user) }
func keyArmyMoves(user string) string       { return key(routing.ArmyMovesPrefix, user) }
func keyWarRecognitions(user string) string { return key(routing.WarRecognitionsPrefix, user) }
func keyGameLogs(user string) string        { return key(routing.GameLogSlug, user) }

func bindArmyMovesPattern() string { return bindAll(routing.ArmyMovesPrefix) }
func bindGameLogsPattern() string  { return bindAll(routing.GameLogSlug) }
