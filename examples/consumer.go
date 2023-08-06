package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/memphisdev/memphis.go"
)

func main1() {
	// conn, err := memphis.Connect("localhost", "root", memphis.Password("memphis"))
	conn, err := memphis.Connect("localhost", "shay", memphis.Password("c6e!#$e7G!1%"))
	// conn, err := memphis.Connect("staging.memphis.dev", "shay", memphis.Password("3#9Uj6J9b?"))
	// conn, err := memphis.Connect("staging.memphis.dev", "shay", memphis.Password("S@$SZk6n$v"), memphis.MaxReconnect(20), memphis.ReconnectInterval(5*time.Second))
	// conn, err := memphis.Connect("staging.memphis.dev", "shay", memphis.Password("9Zw$!bl%3!!tv#%"))
	// conn, err := memphis.Connect("aws-eu-central-1.cloud.memphis.dev", "shaytest", memphis.Password("k@$56#$sRd"), memphis.AccountId(223671990))
	// conn, err := memphis.Connect("aws-eu-central-1.cloud-staging.memphis.dev", "shay", memphis.Password("X6%7P9-nVl"), memphis.AccountId(223672031))

	if err != nil {
		fmt.Printf("Connection failed: %v", err)
		os.Exit(1)
	}
	defer conn.Close()

	consumer, err := conn.CreateConsumer("shaytest3", "c3", memphis.ConsumerGroup("c7"), memphis.PullInterval(5*time.Second), memphis.MaxMsgDeliveries(1))

	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}
	consumer2, err := conn.CreateConsumer("shaytest3", "c3", memphis.ConsumerGroup("c7"), memphis.PullInterval(5*time.Second), memphis.MaxMsgDeliveries(1))

	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}

	consumer3, err := conn.CreateConsumer("shaytest3", "c3", memphis.ConsumerGroup("c8"), memphis.PullInterval(5*time.Second), memphis.MaxMsgDeliveries(1))

	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}
	consumer4, err := conn.CreateConsumer("shaytest3", "c3", memphis.ConsumerGroup("c8"), memphis.PullInterval(5*time.Second), memphis.MaxMsgDeliveries(1))

	if err != nil {
		fmt.Printf("Consumer creation failed: %v\n", err)
		os.Exit(1)
	}

	handler := func(msgs []*memphis.Msg, err error, ctx context.Context) {
		if err != nil {
			fmt.Printf("Fetch failed: %v\n", err)
			return
		}

		for i, msg := range msgs {
			fmt.Println(string(msg.Data()))
			if i%2 != 0 {
				fmt.Println(i)
				msg.Ack()
			}
			headers := msg.GetHeaders()
			fmt.Println(headers)
		}
	}

	consumer.Consume(handler)
	consumer2.Consume(handler)
	consumer3.Consume(handler)
	consumer4.Consume(handler)
	time.Sleep(600 * time.Second)
	err = consumer.Destroy()
	// if err != nil {
	// 	fmt.Println("Destroy consumer failed: " + err.Error())
	// }
	// err = consumer2.Destroy()
	// if err != nil {
	// 	fmt.Println("Destroy consumer2 failed: " + err.Error())
	// }
	// err = consumer3.Destroy()
	// if err != nil {
	// 	fmt.Println("Destroy consumer3 failed: " + err.Error())
	// }
	// err = consumer4.Destroy()
	// if err != nil {
	// 	fmt.Println("Destroy consumer3 failed: " + err.Error())
	// }

	// The program will close the connection after 30 seconds,
	// the message handler may be called after the connection closed
	// so the handler may receive a timeout error
}

// package main

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"log"
// 	"strings"
// 	"time"

// 	m "github.com/memphisdev/memphis.go"
// )

// func main1() {
// 	platform, username, pass := "cloud", "shay", "!##1#Bv5Z0"

// 	log.Println("connect")
// 	conn, err := m.Connect("aws-eu-central-1.cloud-staging.memphis.dev",
// 		username,
// 		m.Password(pass),
// 		m.AccountId(223672012),
// 		m.Reconnect(true),
// 		m.MaxReconnect(10),
// 		m.ReconnectInterval(1500*time.Millisecond),
// 		m.Timeout(1500*time.Millisecond))
// 	if err != nil {
// 		log.Fatal("Error: could not connect")
// 	}
// 	success()

// 	log.Println("creating a station")
// 	sn := fmt.Sprintf("%v.go.station", platform)
// 	s, err := conn.CreateStation(
// 		sn,
// 		m.RetentionTypeOpt(m.MaxMessageAgeSeconds),
// 		m.RetentionVal(604800),
// 		m.StorageTypeOpt(m.Disk),
// 		m.Replicas(1),
// 		m.IdempotencyWindow(time.Duration(0)),
// 		m.SendPoisonMsgToDls(true),
// 		m.SendSchemaFailedMsgToDls(true),
// 	)
// 	if err != nil {
// 		log.Fatal("Error: could not create station")
// 	}
// 	log.Printf("created station %v\n", platform)
// 	success()

// 	log.Println("creating a producer")
// 	pn := fmt.Sprintf("%v.producer", platform)
// 	p, err := s.CreateProducer(pn)
// 	if err != nil {
// 		log.Fatal("Error: could not create producer")
// 	}
// 	log.Printf("created producer %v\n", pn)
// 	success()

// 	log.Println("producing messages")
// 	done := make(chan int)
// 	for i := 0; i < 100; i++ {
// 		go func(i int) {
// 			msg := fmt.Sprintf("Message %v: Hello world", i)
// 			err := p.Produce([]byte(msg))

// 			if err != nil {
// 				log.Fatal("Error: could not produce message")
// 			}

// 			done <- i

// 		}(i)
// 	}

// 	for i := 0; i < 100; i++ {
// 		<-done
// 	}

// 	log.Printf("producer %v produced 100 messages\n", pn)
// 	success()

// 	// create 4 consumers:
// 	// 1 with no consumer group
// 	// 2 same as 1 with a different name
// 	// 3+4 with the same consumer group
// 	log.Println("creating consumers")
// 	consumers := make([]*m.Consumer, 4)
// 	for i := 0; i < 4; i++ {
// 		cn := getValidConsumerName(platform, i+1)
// 		var c *m.Consumer
// 		if i < 2 {
// 			c, err = s.CreateConsumer(cn, m.BatchSize(100))
// 		} else {
// 			c, err = s.CreateConsumer(cn, m.ConsumerGroup("consumers.three.four"), m.BatchSize(100))
// 		}
// 		if err != nil {
// 			log.Fatal("Error: could not create consumer")
// 		}
// 		log.Printf("created %v", cn)
// 		consumers[i] = c
// 	}
// 	success()

// 	// check consumer 1
// 	log.Printf("consume and count messages for consumer %v\n", consumers[0].Name)
// 	n, err := countConsume(consumers[0], nil)
// 	if err != nil {
// 		log.Fatal("consume ended with irregular error:", err.Error())
// 	}
// 	if n != 100 {
// 		log.Fatal("consumer did not consume all messages")
// 	}
// 	log.Println("consumed all messages")
// 	success()

// 	// check consumer 2
// 	log.Printf("consume and count messages for consumer %v\n", consumers[1].Name)
// 	n, err = countConsume(consumers[1], nil)
// 	if err != nil {
// 		log.Fatal("consume ended with irregular error:", err.Error())
// 	}
// 	if n != 100 {
// 		log.Fatal("consumer did not consume all messages")
// 	}
// 	log.Println("consumed all messages")
// 	success()

// 	// check consumers 3, 4
// 	log.Printf("consume and count messages for consumers %v & %v\n", consumers[2].Name, consumers[3].Name)
// 	resMap2 := make(map[string]int)
// 	n2, err := countConsume(consumers[2], resMap2)
// 	if err != nil {
// 		log.Fatal("consume ended with irregular error:", err.Error())
// 	}

// 	resMap3 := make(map[string]int)
// 	n3, err := countConsume(consumers[3], resMap3)
// 	if err != nil {
// 		log.Fatal("consume ended with irregular error:", err.Error())
// 	}
// 	if n2+n3 != 100 {
// 		log.Fatal("consumer did not consume all messages")
// 	}
// 	log.Println("consumed all messages in the consumer group")
// 	success()

// 	log.Println("destroying consumers")
// 	for _, c := range consumers {
// 		cn := c.Name
// 		log.Printf("destroy consumer %v\n", cn)
// 		err := c.Destroy()
// 		if err != nil {
// 			log.Fatal("Error: consumer destruction failed")
// 		}
// 		log.Printf("%v destroyed", cn)
// 	}
// 	success()

// 	log.Printf("destroy producer %v\n", pn)
// 	err = p.Destroy()
// 	if err != nil {
// 		log.Fatal("Error: producer destruction failed")
// 	}
// 	log.Printf("%v destroyed", pn)
// 	success()

// 	log.Printf("destroy station %v\n", s.Name)
// 	err = s.Destroy()
// 	if err != nil {
// 		log.Fatal("Error: station destruction failed")
// 	}
// 	log.Printf("%v destroyed", s.Name)
// 	success()

// 	conn.Close()
// }

// func success() {
// 	log.Println("----Success----")
// }

// func getValidConsumerName(prefix string, n int) string {
// 	suffix := intToRoman(n)
// 	return fmt.Sprintf("%v.consumer.%v", prefix, suffix)
// }

// func intToRoman(n int) string {
// 	if n == 4 {
// 		return "iv"
// 	}
// 	var b strings.Builder
// 	for i := 0; i < n; i++ {
// 		b.WriteString("i")
// 	}
// 	return b.String()
// }

// func countConsume(c *m.Consumer, resMap map[string]int) (int, error) {
// 	counter := 0
// 	errChan := make(chan error, 1)
// 	c.Consume(createMsgHandler(&counter, resMap, errChan))
// 	err := <-errChan
// 	c.StopConsume()
// 	return counter, err
// }

// func createMsgHandler(counter *int, resMap map[string]int, errChan chan error) m.ConsumeHandler {
// 	return func(msgs []*m.Msg, err error, ctx context.Context) {
// 		if err != nil {
// 			errChan <- err
// 			return
// 		}
// 		if len(msgs) == 0 {
// 			errChan <- nil
// 			return
// 		}
// 		*counter += len(msgs)
// 		if resMap != nil {
// 			for _, msg := range msgs {
// 				key := string(msg.Data())
// 				val, ok := resMap[key]
// 				if !ok { // value does not exist
// 					resMap[key] = 1
// 				} else if val > 0 { // we've encountered this value
// 					errChan <- errors.New("value encountered more than once")
// 				}
// 			}
// 		}

// 	}
// }
