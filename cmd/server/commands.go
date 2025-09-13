package main

import (
	"errors"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrQuit = errors.New("quit")

var serverOrder = []string{"pause", "resume", "quit", "help"}

type ServerCommand struct {
	Usage       string
	Description string
	Handler     func(ch *amqp.Channel, args []string) error
}

var ServerCommands map[string]ServerCommand

func init() {
	ServerCommands = map[string]ServerCommand{
		"pause":  {"pause", "Pause the game", handlePause},
		"resume": {"resume", "Resume the game", handleResume},
		"quit":   {"quit", "Shut down the server", handleQuitServer},
		"help":   {"help", "Show help", handleServerHelp},
	}
}

func PrintServerHelp() {
	fmt.Println("Possible commands:")
	for _, name := range serverOrder {
		cmd := ServerCommands[name]
		fmt.Printf("* %s\n", cmd.Usage)
	}
}

func handlePause(ch *amqp.Channel, _ []string) error {
	return publishPlayingState(ch, true)
}

func handleResume(ch *amqp.Channel, _ []string) error {
	return publishPlayingState(ch, false)
}

func handleQuitServer(ch *amqp.Channel, _ []string) error {
	return ErrQuit
}

func handleServerHelp(_ *amqp.Channel, _ []string) error {
	PrintServerHelp()
	return nil
}

func publishPlayingState(ch *amqp.Channel, isPaused bool) error {
	action := "resume"
	if isPaused {
		action = "pause"
	}
	fmt.Printf("Sending %s message...\n", action)
	err := pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: isPaused,
		},
	)
	if err != nil {
		return fmt.Errorf("could not publish: %w", err)
	}
	fmt.Printf("%s message sent!\n", action)
	return nil
}
