package memphis

import (
	"fmt"
	"testing"
)

func TestCreateStation(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
	_, err = f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}
}

func TestRemoveStation(t *testing.T) {
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

	err = s.Remove()
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

	ack, err := p.Produce([]byte("Hey There!"), 15)

	if err != nil {
		t.Error(err)
	}

	fmt.Println("ack received? ", <-ack.Ok())
	// consumer, err := s.CreateConsumer("consumer_a", "", 1000, 10, 5000, 30000, 10)

	// fmt.Println(string(<-consumer.Puller))
}
