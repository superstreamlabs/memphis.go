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
	MaxMsgDeliveries   int
	station            *Station
	subscription       *nats.Subscription
	lastBatch          int
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
	MaxAckTimeMillis string `json:"max_ack_time_ms"`
	MaxMsgDeliveries string `json:"max_msg_deliveries"`
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

	c := Consumer{Name: name,
		ConsumerGroup:      consumerGroup,
		PullIntervalMillis: pullIntervalMillis,
		MaxMsgDeliveries:   maxMsgDeliveries,
		station:            s}
	err = s.getConn().create(&c)
	if err != nil {
		return nil, err
	}

	ackWait := time.Duration(maxAckTimeMillis) * time.Millisecond
	c.subscription, err = s.subscribe(&c,
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxRequestBatch(batchMaxWaitTimeMillis),
		nats.MaxRequestBatch(batchSize))
	if err != nil {
		return nil, err
	}

	c.setupPuller(time.Duration(pullIntervalMillis) * time.Millisecond)
	return &c, err
}

func (consumer *Consumer) setupPuller(pullInterval time.Duration) {
	ticker := time.NewTicker(pullInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				msgs, err := consumer.subscription.Fetch(consumer.lastBatch)
				if err != nil {
					consumer.pullerError <- err
				}
				consumer.lastBatch++
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

func (p *Consumer) Remove() error {
	return p.station.getConn().destroy(p)
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
		Name:         c.Name,
		StationName:  c.station.Name,
		ConnectionId: c.station.getConn().ConnId,
		ConsumerType: "application",
	}
}

func (p *Consumer) getDestructionApiPath() string {
	return "/api/consumers/destroyConsumer"
}

func (p *Consumer) getDestructionReq() any {
	return RemoveConsumerReq{Name: p.Name, StationName: p.station.Name}
}
