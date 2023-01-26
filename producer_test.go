package memphis

import (
	"context"
	"testing"
	"time"
)

func TestCreateProducer(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	defer s.Destroy()

	_, err = s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	_, err = s.CreateProducer("producer_name_a")
	if err == nil {
		t.Error("Producer names has to be unique")
	}

	_, err = s.CreateProducer("producer_name_a", ProducerGenUniqueSuffix())
	if err != nil {
		t.Error(err)
	}
	_, err = s.CreateProducer("producer_name_a", ProducerGenUniqueSuffix())
	if err != nil {
		t.Error(err)
	}

	_, err = c.CreateProducer("station_name_1", "producer_name_b")
	if err != nil {
		t.Error(err)
	}

	_, err = c.CreateProducer("station_name_1", "producer_name_b")
	if err == nil {
		t.Error("Producer names has to be unique")
	}

	//This will create a station
	_, err = c.CreateProducer("station_name_2", "producer_name_a")
	if err != nil {
		t.Error(err)
	}

	c.destroy(&Station{Name: "station_name_2", conn: c})
}

func TestProduce(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	defer s.Destroy()

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	err = p.Produce([]byte("Hey There!"), AckWaitSec(15))

	if err != nil {
		t.Error(err)
	}
}

func TestRemoveProducer(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	defer s.Destroy()

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	err = p.Destroy()

	if err != nil {
		t.Error(err)
	}
}

func TestFetch(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	defer s.Destroy()

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	testMessage := "Hey There!"
	err = p.Produce([]byte(testMessage))

	if err != nil {
		t.Error(err)
	}

	consumer, err := s.CreateConsumer("consumer_a")
	if err != nil {
		t.Error(err)
	}

	msgs, err := consumer.Fetch()
	if err != nil {
		t.Error(err)
	}

	res := string(msgs[0].Data())
	if res != testMessage {
		t.Error("Did not receive exact produced message")
	}

	msgs[0].Ack()

	err = consumer.Destroy()
	if err != nil {
		t.Error(err)
	}
}
func TestConsume(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	defer s.Destroy()

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	testMessage := "Hey There!"
	err = p.Produce([]byte(testMessage))

	if err != nil {
		t.Error(err)
	}

	consumer, err := s.CreateConsumer("consumer_a", PullInterval(1*time.Second))
	if err != nil {
		t.Error(err)
	}

	handler := func(msgs []*Msg, err error, ctx context.Context) {
		res := string(msgs[0].Data())
		if res != testMessage {
			t.Error("Did not receive exact produced message")
		}
		msgs[0].Ack()
	}

	consumer.Consume(handler)

	err = consumer.Destroy()
	if err != nil {
		t.Error(err)
	}
}

func TestCreateConsumer(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	defer s.Destroy()

	_, err = s.CreateConsumer("consumer_name_a")
	if err != nil {
		t.Error(err)
	}

	_, err = s.CreateConsumer("consumer_name_a", ConsumerGroup("consumer_group_3"), PullInterval(1*time.Second), BatchSize(10), BatchMaxWaitTime(5*time.Second), MaxAckTime(30*time.Second), MaxMsgDeliveries(10))
	if err == nil {
		t.Error(err)
	}

	_, err = c.CreateConsumer("station_name_1", "consumer_name_b", ConsumerGroup("consumer_group_g"), PullInterval(1*time.Second), BatchSize(10), BatchMaxWaitTime(5*time.Second), MaxAckTime(30*time.Second), MaxMsgDeliveries(10))
	if err != nil {
		t.Error("Consumer names has to be unique")
	}

	_, err = s.CreateConsumer("consumer_name_a", ConsumerGenUniqueSuffix())
	if err != nil {
		t.Error(err)
	}

	_, err = s.CreateConsumer("consumer_name_a", ConsumerGenUniqueSuffix())
	if err != nil {
		t.Error(err)
	}

	_, err = c.CreateConsumer("station_name_1", "consumer_name_b")
	if err == nil {
		t.Error("Consumer names has to be unique")
	}

	_, err = c.CreateConsumer("station_name_1", "consumer_name_a")
	if err == nil {
		t.Error("Consumer names has to be unique")
	}
}

func TestRemoveConsumer(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	consumer, err := s.CreateConsumer("consumer_a")
	if err != nil {
		t.Error(err)
	}

	err = consumer.Destroy()
	if err != nil {
		t.Error(err)
	}

	err = p.Destroy()
	if err != nil {
		t.Error(err)
	}

	err = s.Destroy()
	if err != nil {
		t.Error(err)
	}
}

func TestFullFlow(t *testing.T) {
	conn, err := Connect("127.0.0.1", "root", "memphis")

	if err != nil {

	}

	defer conn.Close()

	station, err := conn.CreateStation("station_test_name")
	if err != nil {
		t.Errorf("Station creation failed: %v\n", err)

	}

	err = station.Destroy()
	if err != nil {
		t.Errorf("Station destruction failed: %v\n", err)

	}

	station, err = conn.CreateStation("station_test_name")
	if err != nil {
		t.Errorf("Station creation failed: %v\n", err)

	}
	defer station.Destroy()

	p, err := conn.CreateProducer(station.Name, "test_producer")
	if err != nil {
		t.Errorf("Producer creation failed: %v\n", err)

	}
	err = p.Destroy()
	if err != nil {
		t.Errorf("Producer destruction failed: %v\n", err)

	}

	p, err = conn.CreateProducer(station.Name, "test_producer")
	if err != nil {
		t.Errorf("Produce failed: %v\n", err)

	}
	err = p.Produce([]byte("You have a message!"))

	if err != nil {
		t.Errorf("Produce failed: %v\n", err)

	}

	consumer, err := conn.CreateConsumer(station.Name, "consumername", PullInterval(5*time.Minute), MaxAckTime(5*time.Minute), MaxMsgDeliveries(7))

	if err != nil {
		t.Errorf("Consumer creation failed: %v\n", err)

	}

	handlerCh := make(chan struct{})
	handler := func(msgs []*Msg, err error, ctx context.Context) {
		if err != nil {
			t.Errorf("Fetch failed: %v\n", err)
			handlerCh <- struct{}{}
			return
		}

		for _, msg := range msgs {
			msg.Ack()
		}
		handlerCh <- struct{}{}
	}

	consumer.Consume(handler)
	<-handlerCh
	consumer.StopConsume()

	err = consumer.Destroy()
	if err != nil {
		t.Errorf("Consumer destruction failed: %v\n", err)
	}
}
