package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
)


var Communication bool

// Define a struct to hold the message fields.
type Message struct {
	Name    string `json:"name"`
	Email   string `json:"email"`
	Message string `json:"message"`
}

func getRabbitMQConnection() (*amqp.Connection, error) {
	// Connect to RabbitMQ server.
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ server: %v", err)
	}
	return conn, nil
}

func main() {
	// Connect to RabbitMQ server.
	conn, err := getRabbitMQConnection()
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

	// Create a new Gin instance.
	r := gin.Default()

	// Register the routes.
	registerRoutes(r, channel, sendQueue, ackQueue, ackMsgs)

	// Start the server.
	go func (){
	log.Fatal(r.Run(":3939"))
	}()
	// Generate a POST request to the route.
	requestBody := []byte(`{"message": "hello world"}`)
	resp, err := http.Post("http://localhost:3939", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body.
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response: %v", err)
	}

	// Print the response body.
	fmt.Println(string(responseBody))
}

func sendMessage(channel *amqp.Channel, sendQueue amqp.Queue, message Message, ackQueueName string) error {
	// Encode the message as JSON.
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Publish the message to the queue.
	err = channel.Publish(
		"",             // exchange
		sendQueue.Name, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			ReplyTo:     ackQueueName,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func registerRoutes(router *gin.Engine, channel *amqp.Channel, sendQueue amqp.Queue, ackQueue amqp.Queue, ackMsgs <-chan amqp.Delivery) {
	// Handle incoming requests.
	router.POST("/", func(c *gin.Context) {
		// Parse the JSON message from the request body.
		var message Message
		if err := c.BindJSON(&message); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to parse message.",
			})
			return
		}

		// Send the message to the consumer.
		err := sendMessage(channel, sendQueue, message, ackQueue.Name)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to publish message.",
			})
			return
		}

		// Wait for the acknowledgement or timeout.
		select {
		case <-time.After(2 * time.Second):
			fmt.Println("Your response has been received, processed soon!")
			c.JSON(http.StatusOK, gin.H{
				"message": "No acknowledgement received.",
			})
		case <-ackMsgs:
			fmt.Println("Acknowledgement received:")
			c.JSON(http.StatusOK, gin.H{
				"message": "Acknowledgement received.",
			})
		}
	})
}
