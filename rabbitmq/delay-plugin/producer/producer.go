package main

import (
	"log"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	body := []byte("hello")
	if err = ch.Publish(
		"test_task_exchange2",
		"",
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"x-delay": 3000,
			},
			ContentType: "text/plain",
			Body:        body,
		}); err != nil {
		panic(err)
	}
	log.Printf("Publich message: %s", string(body))
}
