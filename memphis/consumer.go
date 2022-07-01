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

type ConsumerOpts struct {
	Name                   string
	StationName            string
	ConsumerGroup          string
	PullIntervalMillis     int
	BatchSize              int
	BatchMaxWaitTimeMillis int
	MaxAckTimeMillis       int
	MaxMsgDeliveries       int
}

func GetDefaultProducerOptions() ConsumerOpts {
	return ConsumerOpts{
		ConsumerGroup:          "",
		PullIntervalMillis:     1000,
		BatchSize:              10,
		BatchMaxWaitTimeMillis: 5000,
		MaxAckTimeMillis:       30000,
		MaxMsgDeliveries:       10,
	}
}

type ConsumerOpt func(*ConsumerOpts) error

func (c *Conn) CreateConsumer(name string, stationName string, opts ...ConsumerOpt) (*Consumer, error) {
	defaultOpts := GetDefaultProducerOptions()

	err := validateConsumerName(name)
	if err != nil {
		return nil, err
	}

	defaultOpts.Name = name
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

	if opts.ConsumerGroup == "" {
		opts.ConsumerGroup = opts.Name
	}

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

	ackWait := time.Duration(opts.MaxAckTimeMillis) * time.Millisecond
	subj := getSubjectName(opts.StationName)
	consumer.subscription, err = c.brokerSubscribe(subj, opts.ConsumerGroup,
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxRequestBatch(opts.BatchMaxWaitTimeMillis),
		nats.MaxRequestBatch(opts.BatchSize))
	if err != nil {
		return nil, err
	}

	consumer.startPuller(time.Duration(opts.PullIntervalMillis) * time.Millisecond)
	return &consumer, err
}

func (s *Station) CreateConsumer(name string, opts ...ConsumerOpt) (*Consumer, error) {
	return s.getConn().CreateConsumer(name, s.Name, opts...)
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
		opts.BatchMaxWaitTimeMillis = batchMaxWaitTimeMillis
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
