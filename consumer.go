package memphis

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

type Consumer struct {
	Name               string
	ConsumerGroup      string
	PullIntervalMillis int
	BatchSize          int
	MaxAckTimeMillis   int
	MaxMsgDeliveries   int
	BatchMaxTimeToWait time.Duration
	conn               *Conn
	stationName        string
	subscription       *nats.Subscription
	firstFetch         bool
	consumeActive      bool
	puller             chan *Msg
	pullerQuit         chan struct{}
}

type Msg struct {
	msg *nats.Msg
}

func (m *Msg) Data() []byte {
	return m.msg.Data
}

func (m *Msg) Ack() error {
	return m.msg.Ack()
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
	defaultOpts.ConsumerGroup = consumerName

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
		BatchSize:          opts.BatchSize,
		MaxAckTimeMillis:   opts.MaxAckTimeMillis,
		MaxMsgDeliveries:   opts.MaxMsgDeliveries,
		BatchMaxTimeToWait: time.Duration(opts.BatchMaxTimeToWaitMillis) * time.Millisecond,
		conn:               c,
		stationName:        opts.StationName}

	err := c.create(&consumer)
	if err != nil {
		return nil, err
	}

	consumer.firstFetch = true
	consumer.puller = make(chan *Msg, consumer.BatchSize)
	consumer.pullerQuit = make(chan struct{}, 1)

	ackWait := time.Duration(consumer.MaxAckTimeMillis) * time.Millisecond
	subj := consumer.stationName + ".final"
	durableName := consumer.ConsumerGroup
	if durableName == "" {
		durableName = consumer.Name
	}

	consumer.subscription, err = c.brokerSubscribe(subj, durableName,
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxRequestExpires(consumer.BatchMaxTimeToWait),
		nats.MaxRequestBatch(opts.BatchSize))
	if err != nil {
		return nil, err
	}

	return &consumer, err
}

func (s *Station) CreateConsumer(name string, opts ...ConsumerOpt) (*Consumer, error) {
	return s.conn.CreateConsumer(s.Name, name, opts...)
}

type ConsumeHandler func([]*Msg, error)

func (c *Consumer) Consume(pullInterval time.Duration, handlerFunc ConsumeHandler) {
	ticker := time.NewTicker(pullInterval)

	if c.firstFetch {
		c.firstFetch = false
		msgs, err := fetchSubscription(c.subscription, c.BatchSize)
		go handlerFunc(msgs, err)
	}

	go func() {
		for {
			select {
			case <-ticker.C:
				msgs, err := fetchSubscription(c.subscription, c.BatchSize)
				go handlerFunc(msgs, err)
			case <-c.pullerQuit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Consumer) StopConsume() {
	c.pullerQuit <- struct{}{}
}

func fetchSubscription(subscription *nats.Subscription, batchSize int) ([]*Msg, error) {
	msgs, err := subscription.Fetch(batchSize)
	if err != nil {
		return nil, err
	}

	wrappedMsgs := make([]*Msg, 0, batchSize)

	for _, msg := range msgs {
		wrappedMsgs = append(wrappedMsgs, &Msg{msg: msg})
	}
	return wrappedMsgs, nil
}

type FetchResult struct {
	msgs []*Msg
	err  error
}

func fetchSubscriprionWithTimeout(subscription *nats.Subscription, batchSize int, timeoutDuration time.Duration) ([]*Msg, error) {
	out := make(chan FetchResult, 1)
	go func() {
		msgs, err := fetchSubscription(subscription, batchSize)
		out <- FetchResult{msgs: msgs, err: err}
	}()
	select {
	case <-time.After(timeoutDuration):
		return nil, errors.New("Fetch timed out")
	case fetchRes := <-out:
		return fetchRes.msgs, fetchRes.err

	}
}

func (c *Consumer) Fetch() ([]*Msg, error) {
	if c.firstFetch {
		c.firstFetch = false
	}

	return fetchSubscriprionWithTimeout(c.subscription, c.BatchSize, c.BatchMaxTimeToWait)
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
