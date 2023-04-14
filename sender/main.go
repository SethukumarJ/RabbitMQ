package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/streadway/amqp"
)

// Define a struct to hold the message fields.
type Message struct {
	Name    string `json:"name"`
	Email   string `json:"email"`
	Message string `json:"message"`
}

func main() {
	// Connect to RabbitMQ server.
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ server: %v", err)
	}
	defer conn.Close()

	// Create a channel.
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer channel.Close()

	// Declare a queue for sending messages.
	sendQueue, err := channel.QueueDeclare(
		"send_queue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue for sending messages: %v", err)
	}

	// Declare a queue for receiving acknowledgements.
	ackQueue, err := channel.QueueDeclare(
		"ack_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue for receiving acknowledgements: %v", err)
	}

	// Start consuming acknowledgements.
	ackMsgs, err := channel.Consume(
		ackQueue.Name, // queue
		"",            // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer for acknowledgements: %v", err)
	}
	go func() {
		for ack := range ackMsgs {
			fmt.Println("Acknowledgement received:", string(ack.Body))
		}
	}()

	// Create a new Fiber instance.
	app := fiber.New()

// Handle incoming requests.
app.Post("/", func(c *fiber.Ctx) error {
	// Parse the JSON message from the request body.
	var message Message
	if err := json.Unmarshal(c.Body(), &message); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse message.",
		})
	}

	// Send the message to the consumer.
	body, err := json.Marshal(message)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to encode message.",
		})
	}

	err = channel.Publish(
		"",             // exchange
		sendQueue.Name, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			ReplyTo:     ackQueue.Name,
		},
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to publish message.",
		})
	}

	// Start a separate goroutine to wait for the acknowledgement.
	done := make(chan bool, 1)
	go func() {
		select {
		case <-time.After(2 * time.Second):
			fmt.Println("Received your request, will be proceeded soon")
			done <- true
		case <-ackMsgs:
			// Do nothing, as the acknowledgement has been received.
			done <- false
		}
	}()

	// Wait for the acknowledgement or timeout.
	if <-done {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"message": "Received your request, will be proceeded soon",
		})
	}

	// Respond with a success message.
	return c.JSON(fiber.Map{
		"message": "Message sent successfully.",
	})
})


	// Start the server.
	log.Fatal(app.Listen(":3939"))
}
