package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gofiber/fiber"
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

	// Declare a queue for receiving messages.
	queue, err := channel.QueueDeclare(
		"send_queue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue for receiving messages: %v", err)
	}


	// Start consuming messages.
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

		// Declare a queue for receiving messages.
		queue1, err := channel.QueueDeclare(
			"ak_que", // name
			false,        // durable
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		if err != nil {
			log.Fatalf("Failed to declare a queue for receiving messages: %v", err,queue1)
		}
	
		
	
	// Loop through incoming messages.
	for msg := range msgs {
		// Parse the JSON message.
		var message Message
		if err := json.Unmarshal(msg.Body, &message); err != nil {
			log.Printf("Failed to parse message: %v", err)
			msg.Nack(false, false)
			continue
		}

		// Print the message fields.
		fmt.Println("Message received:")
		fmt.Println("Name:", message.Name)
		fmt.Println("Email:", message.Email)
		fmt.Println("Message:", message.Message)

		// Send an acknowledgment.
		body, err := json.Marshal(fiber.Map{
			"message": "Received message successfully.",
		})
		if err != nil {
			log.Printf("Failed to encode acknowledgment: %v", err)
			continue
		}

		err = channel.Publish(
			"",          // exchange
			msg.ReplyTo, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			log.Printf("Failed to send acknowledgment: %v", err)
			msg.Nack(false, false)
			continue
		}
		err = channel.Publish(
			"",          // exchange
			"ak_que", // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			log.Printf("Failed to send acknowledgment: %v", err)
			msg.Nack(false, false)
			continue
		}

		// Acknowledge the message.
		msg.Ack(false)
	}
	
}
