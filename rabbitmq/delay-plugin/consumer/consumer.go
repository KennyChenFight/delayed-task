package main

import (
	"log"

	"github.com/streadway/amqp"
)

const (
	testTaskExchangeName string = "test_task_exchange2"
	testTaskQueueName    string = "test_task_queue2"
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

	if err = ch.ExchangeDeclare(
		testTaskExchangeName,
		"x-delayed-message",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-delayed-type": "fanout",
		},
	); err != nil {
		panic(err)
	}

	testTaskQueue, err := ch.QueueDeclare(
		testTaskQueueName,
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	if err := ch.QueueBind(
		testTaskQueue.Name,
		"",
		testTaskExchangeName,
		false,
		nil); err != nil {
		panic(err)
	}

	msgs, err := ch.Consume(
		testTaskQueue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("receive message: %s", string(d.Body))
		}
	}()
	log.Printf("Waiting for test_task_queue message.")
	<-forever
}
