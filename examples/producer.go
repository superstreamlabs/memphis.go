package main

import (
	"fmt"
	"os"
	"time"

	"github.com/memphisdev/memphis.go"
)

func main() {
	conn, err := memphis.Connect("localhost", "shay", memphis.Password("c6e!#$e7G!1%"))
	// conn, err := memphis.Connect("localhost", "root", memphis.Password("memphis"))
	// conn, err := memphis.Connect("staging.memphis.dev", "shay", memphis.Password("S@$SZk6n$v"), memphis.MaxReconnect(20), memphis.ReconnectInterval(5*time.Second))
	// conn, err := memphis.Connect("aws-eu-central-1.cloud-staging.memphis.dev", "shay", memphis.Password("X6%7P9-nVl"), memphis.AccountId(223672031))
	// conn, err := memphis.Connect("aws-eu-central-1.cloud.memphis.dev", "shaytest", memphis.Password("k@$56#$sRd"), memphis.AccountId(223671990))
	// conn, err := memphis.Connect("staging.memphis.dev", "shay", memphis.Password("oCk7nZ7@E@"))
	if err != nil {
		fmt.Printf("Connection failed: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	// _, err = conn.CreateStation("shaytest", memphis.Replicas(3))
	// if err != nil {
	// 	fmt.Printf("Create Station failed: %v", err)
	// 	os.Exit(1)
	// }
	p, err := conn.CreateProducer("shaytest3", "producer2")

	if err != nil {
		fmt.Printf("Create Producer failed: %v", err)
		os.Exit(1)
	}
	hdrs := memphis.Headers{}
	hdrs.New()
	err = hdrs.Add("key", "value")

	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}
	for {
		for i := 0; i < 100; i++ {
			err = p.Produce([]byte("!@#$%^&*()_+-=±§~/.,><\n|/*sd][}{"), memphis.MsgHeaders(hdrs))
			if err != nil {
				fmt.Printf("Produce failed: %v", err)
				// os.Exit(1)
			}
		}
		time.Sleep(1 * time.Second)
	}
	p2, err := conn.CreateProducer("cloud.go.station3", "producer2")

	if err != nil {
		fmt.Printf("Create Producer failed: %v", err)
		os.Exit(1)
	}

	hdrs2 := memphis.Headers{}
	hdrs2.New()
	err = hdrs2.Add("key", "value")

	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}

	err = p2.Produce([]byte("You have a message!"), memphis.MsgHeaders(hdrs2))

	if err != nil {
		fmt.Printf("Produce2 failed: %v", err)
		// os.Exit(1)
	}

	time.Sleep(30 * time.Second)
	// err = p.Destroy()
	// if err != nil {
	// 	fmt.Printf("p Destroy failed: %v", err)
	// }

	// err = p2.Destroy()
	// if err != nil {
	// 	fmt.Printf("p2 Destroy failed: %v", err)
	// 	os.Exit(1)
	// }

}

// package main

// import (
// 	"fmt"
// 	"log"
// 	"time"

// 	"github.com/nats-io/nats.go"
// )

// func main() {
// 	// Connect to the NATS server
// 	// nc, err := nats.Connect("nats://root:kW2FHLc8YapfRVCUdCO6@staging.memphis.dev:6666")
// 	nc, err := nats.Connect("nats://localhost:1234")
// 	if err != nil {
// 		log.Fatalf("Error connecting to NATS server: %v", err)
// 	}
// 	defer nc.Close()

// 	// js, _ := jetstream.New(nc)
// 	js, _ := nc.JetStream()

// 	// Create a subscription to the $SYS.STREAM.INFO.<stream_name> subject
// 	// streamName := "cloud#go#station3" // Replace this with the actual stream name
// 	// streamName := "shay2" // Replace this with the actual stream name
// 	// subject := fmt.Sprintf("$SYS.STREAM.INFO.%s", streamName)
// 	subject := "shay2.final"
// 	data := []byte("your_message_data")
// 	msg := &nats.Msg{
// 		Subject: subject,
// 		Data:    data,
// 		Header:  map[string][]string{"key": {"value"}},
// 	}
// 	stallWaitDuration := time.Second * 15

// 	for {
// 		for i := 0; i < 100; i++ {
// 			_, err := js.PublishMsgAsync(msg, nats.StallWait(stallWaitDuration))
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			fmt.Println("pub")
// 		}
// 		time.Sleep(200 * time.Millisecond)
// 	}

// 	// Subscribe to the subject
// 	// _, err = nc.Subscribe(subject, func(msg *nats.Msg) {
// 	// 	// fmt.Printf("Received Stream Info for %s:\n%s\n", streamName, msg.Data)
// 	// 	fmt.Println(msg)
// 	// })
// 	// if err != nil {
// 	// 	log.Fatalf("Error subscribing to subject %s: %v", subject, err)
// 	// }
// 	// for {
// 	// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	// 	streams := js.ListStreams(ctx)
// 	// 	for s := range streams.Info() {
// 	// 		fmt.Println(s.Config.Name)
// 	// 	}
// 	// 	cancel()
// 	// 	time.Sleep(100 * time.Millisecond)
// 	// }

// 	// Wait for some time to receive the stream info (adjust this as needed)
// 	// time.Sleep(5 * time.Second)
// }
