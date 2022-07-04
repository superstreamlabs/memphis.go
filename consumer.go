package memphis

import (
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

type createConsumerReq struct {
	Name             string `json:"name"`
	StationName      string `json:"station_name"`
	ConnectionId     string `json:"connection_id"`
	ConsumerType     string `json:"consumer_type"`
	ConsumerGroup    string `json:"consumers_group"`
	MaxAckTimeMillis int    `json:"max_ack_time_ms"`
	MaxMsgDeliveries int    `json:"max_msg_deliveries"`
}

type removeConsumerReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
}

type ConsumerOpts struct {
	Name                     string
	StationName              string
	ConsumerGroup            string
	PullIntervalMillis       int
	BatchSize                int
	BatchMaxTimeToWaitMillis int
	MaxAckTimeMillis         int
	MaxMsgDeliveries         int
}

func GetDefaultConsumerOptions() ConsumerOpts {
	return ConsumerOpts{
		ConsumerGroup:            "",
		PullIntervalMillis:       1000,
		BatchSize:                10,
		BatchMaxTimeToWaitMillis: 5000,
		MaxAckTimeMillis:         30000,
		MaxMsgDeliveries:         10,
	}
}

type ConsumerOpt func(*ConsumerOpts) error

func (c *Conn) CreateConsumer(stationName, consumerName string, opts ...ConsumerOpt) (*Consumer, error) {
	defaultOpts := GetDefaultConsumerOptions()

	defaultOpts.Name = consumerName
	defaultOpts.StationName = stationName

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, err
			}
		}
	}

	return defaultOpts.CreateConsumer(c)
}

func (opts *ConsumerOpts) CreateConsumer(c *Conn) (*Consumer, error) {
	consumer := Consumer{Name: opts.Name,
		ConsumerGroup:      opts.ConsumerGroup,
		PullIntervalMillis: opts.PullIntervalMillis,
		MaxAckTimeMillis:   opts.MaxAckTimeMillis,
		MaxMsgDeliveries:   opts.MaxMsgDeliveries,
		conn:               c,
		stationName:        opts.StationName}

	err := c.create(&consumer)
	if err != nil {
		return nil, err
	}

	consumer.Puller = make(chan []byte, 1024)
	consumer.pullerQuit = make(chan struct{}, 1)

	ackWait := time.Duration(consumer.MaxAckTimeMillis) * time.Millisecond
	subj := getSubjectName(consumer.stationName)
	durableName := consumer.ConsumerGroup
	if durableName == "" {
		durableName = consumer.Name
	}

	consumer.subscription, err = c.brokerSubscribe(subj, durableName,
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxRequestExpires(time.Duration(opts.BatchMaxTimeToWaitMillis)*time.Millisecond),
		nats.MaxRequestBatch(opts.BatchSize))
	if err != nil {
		return nil, err
	}

	consumer.startPuller(time.Duration(opts.PullIntervalMillis) * time.Millisecond)
	return &consumer, err
}

func (s *Station) CreateConsumer(name string, opts ...ConsumerOpt) (*Consumer, error) {
	return s.conn.CreateConsumer(s.Name, name, opts...)
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

func (c *Consumer) Destroy() error {
	c.pullerQuit <- struct{}{}
	return c.conn.destroy(c)
}

func (c *Consumer) getCreationApiPath() string {
	return "/api/consumers/createConsumer"
}

func (c *Consumer) getCreationReq() any {
	return createConsumerReq{
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
	return removeConsumerReq{Name: p.Name, StationName: p.stationName}
}

func ConsumerName(name string) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.Name = name
		return nil
	}
}

func StationNameOpt(stationName string) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.StationName = stationName
		return nil
	}
}
func ConsumerGroup(cg string) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.ConsumerGroup = cg
		return nil
	}
}
func PullIntervalMillis(pullIntervalMillis int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.PullIntervalMillis = pullIntervalMillis
		return nil
	}
}
func BatchSize(batchSize int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.BatchSize = batchSize
		return nil
	}
}
func BatchMaxWaitTimeMillis(batchMaxWaitTimeMillis int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.BatchMaxTimeToWaitMillis = batchMaxWaitTimeMillis
		return nil
	}
}
func MaxAckTimeMillis(maxAckTimeMillis int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.MaxAckTimeMillis = maxAckTimeMillis
		return nil
	}
}
func MaxMsgDeliveries(maxMsgDeliveries int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.MaxMsgDeliveries = maxMsgDeliveries
		return nil
	}
}
