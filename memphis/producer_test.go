package memphis

import (
	"fmt"
	"testing"
)

func TestCreateProducer(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}

	defer f.Remove()

	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	_, err = s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	//
	_, err = s.CreateProducer("producer_name_a")
	if err == nil {
		t.Error("Producer names has to be unique")
	}

	_, err = c.CreateProducer("producer_name_b", "station_name_1")
	if err != nil {
		t.Error(err)
	}

	_, err = c.CreateProducer("producer_name_b", "station_name_1")
	if err == nil {
		t.Error("Producer names has to be unique")
	}

	_, err = c.CreateProducer("producer_name_a", "station_name_2")
	if err != nil {
		t.Error(err)
	}
}

func TestProduce(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}

	defer f.Remove()

	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	_, err = p.Produce([]byte("Hey There!"), 15)

	if err != nil {
		t.Error(err)
	}
}

func TestValidateProducerName(t *testing.T) {
	err := validateProducerName("val_id")
	if err != nil {
		t.Error(err)
	}

	err = validateProducerName("inVALID")
	if err == nil {
		t.Error(err)
	}

	err = validateProducerName("invalid1")
	if err == nil {
		t.Error(err)
	}
}

func TestRemoveProducer(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	err = p.Remove()

	if err != nil {
		t.Error(err)
	}
}

func TestConsume(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	testMessage := "Hey There!"
	ack, err := p.Produce([]byte(testMessage), 15)

	if err != nil {
		t.Error(err)
	}

	fmt.Println("ack received? ", <-ack.Ok())

	consumer, err := s.CreateConsumer("consumer_a", "", 1000, 10, 5000, 30000, 10)
	res, ok := <-consumer.Puller
	if !ok {
		t.Error("chan is not ok")
	}

	if string(res) != testMessage {
		t.Error("Did not receive exact produced message")
	}

	fmt.Println("what we got from the channel:", string(res), ok)
	err = consumer.Remove()
	if err != nil {
		t.Error(err)
	}

	f.Remove()
	c.Close()
}

func TestCreateConsumer(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}

	defer f.Remove()

	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	_, err = s.CreateConsumer("consumer_name_a", "", 1000, 10, 5000, 30000, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = s.CreateConsumer("consumer_name_a", "", 1000, 10, 5000, 30000, 10)
	if err == nil {
		t.Error(err)
	}

	_, err = c.CreateConsumer("consumer_name_b", "station_name_1", "", 1000, 10, 5000, 30000, 10)
	if err != nil {
		t.Error("Consumer names has to be unique")
	}

	_, err = c.CreateConsumer("consumer_name_b", "station_name_1", "", 1000, 10, 5000, 30000, 10)
	if err == nil {
		t.Error("Consumer names has to be unique")
	}

	_, err = c.CreateConsumer("consumer_name_a", "station_name_1", "", 1000, 10, 5000, 30000, 10)
	if err == nil {
		t.Error("Consumer names has to be unique")
	}
}

func TestRemoveConsumer(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	p, err := s.CreateProducer("producer_name_a")
	if err != nil {
		t.Error(err)
	}

	consumer, err := s.CreateConsumer("consumer_a", "", 1000, 10, 5000, 30000, 10)
	if err != nil {
		t.Error(err)
	}

	err = consumer.Remove()
	if err != nil {
		t.Error(err)
	}

	err = p.Remove()
	if err != nil {
		t.Error(err)
	}

	err = s.Remove()
	if err != nil {
		t.Error(err)
	}

	err = f.Remove()
	if err != nil {
		t.Error(err)
	}

	c.Close()
}
