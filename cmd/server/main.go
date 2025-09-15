package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not open new channel: %v", err)
	}
	defer publishCh.Close()

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		handlerLogs(),
	)
	if err != nil {
		log.Fatalf("could not starting consuming logs: %v", err)
	}

	PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		name, args := input[0], input[1:]
		cmd, ok := ServerCommands[name]
		if !ok {
			fmt.Printf("unknown command: %s\n", name)
			continue
		}
		if err := cmd.Handler(publishCh, args); err != nil {
			if errors.Is(err, ErrQuit) {
				fmt.Println("Shutting down server...")
				break
			}
			fmt.Printf("error: %v\n", err)
		}
	}
}
