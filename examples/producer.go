package main

import (
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
		return
	}

	defer conn.Close()

	message := make(map[string]any)

	message["Hello"] = "World"
	for i := 0; i < 3; i++{
		err = conn.Produce(
			"test_station",
			"producer",
			message, 
			[]memphis.ProducerOpt{}, 
			[]memphis.ProduceOpt{},
		)
	}
		
	if err != nil{
		return
	}
}