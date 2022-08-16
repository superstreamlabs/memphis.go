package main

import (
	"fmt"
	"os"
	"time"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("127.0.0.1", "root", "memphis")

	if err != nil {
		os.Exit(1)
	}

	defer conn.Close()

	factory, err := conn.CreateFactory("factory_test")
	if err != nil {
		fmt.Printf("Factory creation failed: %v\n", err)
		os.Exit(2)
	}

	station, err := conn.CreateStation("station_test_name", factory.Name)
	fmt.Println(station)
	if err != nil {
		fmt.Printf("Station creation failed: %v\n", err)
		os.Exit(3)
	}


	p, err := conn.CreateProducer(station.Name, "test_producer")
	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(4)
	}
	err = p.Produce([]byte("You have a message!"))

	if err != nil {
		fmt.Printf("Produce failed: %v\n", err)
		os.Exit(5)
	}

	consumer, err := conn.CreateConsumer(station.Name, "consumername", memphis.PullInterval(5*time.Minute), memphis.MaxAckTime(5*time.Minute), memphis.MaxMsgDeliveries(7))

	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}

	handler := func(msgs []*memphis.Msg, err error) {
		fmt.Println("msgs", msgs)
		if err != nil {
			fmt.Printf("Fetch failed: %v\n", err)
			return
		}

		for _, msg := range msgs {
			fmt.Println("msg", string(msg.Data()))
			msg.Ack()
		}
	}

	consumer.Consume(handler)

}
