package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/memphisdev/memphis.go"
)

func main(){
	accountID, _ := strconv.Atoi(os.Getenv("memphis_account_id"))

	conn, err := memphis.Connect(
		"aws-us-east-1.cloud.memphis.dev",
		"test_user",
		memphis.AccountId(accountID),
		memphis.Password(os.Getenv("memphis_pass")),
	)

	if err != nil{
		fmt.Print(err)
		return
	}

	defer conn.Close()

	producer, err := conn.CreateProducer("test_station", "producer")

	if err != nil{
		fmt.Print(err)
		return
	}

	message := make(map[string]any)

	message["Hello"] = "World"
	for i := 0; i < 3; i++{
		err = producer.Produce(message)
	}
		
	if err != nil{
		return
	}
}