package main

import (
	"fmt"
	"os"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("127.0.0.1", "root", "memphis")

	if err != nil {
		os.Exit(1)
	}

	defer conn.Close()

	factory, err := conn.CreateFactory("factory_test_2")
	if err != nil {
		fmt.Printf("Factory creation failed: %v\n", err)
		os.Exit(2)
	}

	station, err := conn.CreateStation("station_test_name_2", factory.Name)
	fmt.Println(station)
	if err != nil {
		fmt.Printf("Station creation failed: %v\n", err)
		os.Exit(3)
	}

	// p, err := conn.CreateProducer("teststation", "testproducer")
	// if err != nil {
	// 	fmt.Printf("Produce failed: %v\n", err)
	// 	os.Exit(2)
	// }
	// err = p.Produce([]byte("You have a message!"))

	// if err != nil {
	// 	fmt.Printf("Produce failed: %v\n", err)
	// 	os.Exit(3)
	// }
}
