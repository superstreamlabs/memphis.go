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
	conn               *Conn
	stationName        string
	subscription       *nats.Subscription
	Puller             chan []byte
	pullerQuit         chan struct{}
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

func (c *Conn) CreateConsumer(name,
	stationName,
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

	consumer := Consumer{Name: name,
		ConsumerGroup:      consumerGroup,
		PullIntervalMillis: pullIntervalMillis,
		MaxAckTimeMillis:   maxAckTimeMillis,
		MaxMsgDeliveries:   maxMsgDeliveries,
		conn:               c,
		stationName:        stationName}

	err = c.create(&consumer)
	if err != nil {
		return nil, err
	}

	consumer.Puller = make(chan []byte, 1024)
	consumer.pullerQuit = make(chan struct{}, 1)

	ackWait := time.Duration(maxAckTimeMillis) * time.Millisecond
	subj := getSubjectName(stationName)
	consumer.subscription, err = c.brokerSubscribe(subj, consumerGroup,
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxRequestBatch(batchMaxWaitTimeMillis),
		nats.MaxRequestBatch(batchSize))
	if err != nil {
		return nil, err
	}

	consumer.startPuller(time.Duration(pullIntervalMillis) * time.Millisecond)
	return &consumer, err
}

func (s *Station) CreateConsumer(name,
	consumerGroup string,
	pullIntervalMillis,
	batchSize,
	batchMaxWaitTimeMillis,
	maxAckTimeMillis,
	maxMsgDeliveries int) (*Consumer, error) {
	return s.getConn().CreateConsumer(name, s.Name, consumerGroup, pullIntervalMillis, batchSize, batchMaxWaitTimeMillis, maxAckTimeMillis, maxMsgDeliveries)
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
	c.pullerQuit <- struct{}{}
	return c.conn.destroy(c)
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
		StationName:      c.stationName,
		ConnectionId:     c.conn.ConnId,
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
	return RemoveConsumerReq{Name: p.Name, StationName: p.stationName}
}
