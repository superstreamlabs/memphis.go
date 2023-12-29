package main

import (
	"encoding/json"
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

	consumer, _ := conn.CreateConsumer("test_station", "consumer")

	messages, err := consumer.Fetch()

	if err != nil{
		fmt.Print(err)
		return
	}

	var msg_map map[string]any
	for _, message := range messages{
		err = json.Unmarshal(message.Data(), &msg_map)
		if err != nil{
			fmt.Print(err)
			continue
		}

		// Do something with the message

		err = message.Ack()
		if err != nil{
			fmt.Print(err)
			continue
		}
	}
}