package main

import (
	"log"

	"github.com/streadway/amqp"
)

const (
	testTaskExchangeName string = "test_task_exchange"
	testTaskQueueName    string = "test_task_queue"
	testDelayQueueName   string = "test_delay_queue"
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
		"fanout",
		true,
		false,
		false,
		false,
		nil,
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

	if _, err := ch.QueueDeclare(
		testDelayQueueName,
		false,
		false,
		true,
		false,
		amqp.Table{
			"x-dead-letter-exchange": testTaskExchangeName,
		},
	); err != nil {
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
