package memphis

import (
	"errors"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
)

type Consumer struct {
	Name               string
	ConsumerGroup      string
	PullIntervalMillis int
	MaxAckTimeMillis   int
	MaxMsgDeliveries   int
	station            *Station
	subscription       *nats.Subscription
	Puller             chan []byte
	pullerQuit         chan bool
	pullerError        chan error
}

type CreateConsumerReq struct {
	Name             string `json:"name"`
	StationName      string `json:"station_name"`
	ConnectionId     string `json:"connection_id"`
	ConsumerType     string `json:"consumer_type"`
	ConsumerGroup    string `json:"consumers_group"`
	MaxAckTimeMillis int    `json:"max_ack_time_ms"`
	MaxMsgDeliveries int    `json:"max_msg_deliveries"`
}

type RemoveConsumerReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
}

func (s *Station) CreateConsumer(name,
	consumerGroup string,
	pullIntervalMillis,
	batchSize,
	batchMaxWaitTimeMillis,
	maxAckTimeMillis,
	maxMsgDeliveries int) (*Consumer, error) {
	err := validateConsumerName(name)
	if err != nil {
		return nil, err
	}

	if consumerGroup == "" {
		consumerGroup = name
	}

	c := Consumer{Name: name,
		ConsumerGroup:      consumerGroup,
		PullIntervalMillis: pullIntervalMillis,
		MaxAckTimeMillis:   maxAckTimeMillis,
		MaxMsgDeliveries:   maxMsgDeliveries,
		station:            s}
	err = s.getConn().create(&c)
	if err != nil {
		return nil, err
	}

	c.Puller = make(chan []byte, 1024)
	c.pullerQuit = make(chan bool, 1024)

	ackWait := time.Duration(maxAckTimeMillis) * time.Millisecond
	c.subscription, err = s.subscribe(&c,
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxRequestBatch(batchMaxWaitTimeMillis),
		nats.MaxRequestBatch(batchSize))
	if err != nil {
		return nil, err
	}

	c.startPuller(time.Duration(pullIntervalMillis) * time.Millisecond)
	return &c, err
}

func (consumer *Consumer) startPuller(pullInterval time.Duration) {
	ticker := time.NewTicker(pullInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				msgs, err := consumer.subscription.Fetch(1)
				if err != nil {
					consumer.pullerError <- err
					continue
				}

				for _, msg := range msgs {
					consumer.Puller <- msg.Data
				}
			case <-consumer.pullerQuit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Consumer) Remove() error {
	c.pullerQuit <- true
	return c.station.getConn().destroy(c)
}

func validateConsumerName(name string) error {
	regex := regexp.MustCompile("^[a-z_]+$")
	if !regex.MatchString(name) {
		return errors.New("Consumer name can only contain lower-case letters and _")
	}
	return nil
}

func (c *Consumer) getCreationApiPath() string {
	return "/api/consumers/createConsumer"
}

func (c *Consumer) getCreationReq() any {
	return CreateConsumerReq{
		Name:             c.Name,
		StationName:      c.station.Name,
		ConnectionId:     c.station.getConn().ConnId,
		ConsumerType:     "application",
		ConsumerGroup:    c.ConsumerGroup,
		MaxAckTimeMillis: c.MaxAckTimeMillis,
		MaxMsgDeliveries: c.MaxMsgDeliveries,
	}
}

func (p *Consumer) getDestructionApiPath() string {
	return "/api/consumers/destroyConsumer"
}

func (p *Consumer) getDestructionReq() any {
	return RemoveConsumerReq{Name: p.Name, StationName: p.station.Name}
}
