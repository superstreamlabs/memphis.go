// Credit for The NATS.IO Authors
// Copyright 2021-2022 The Memphis Authors
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.package server

package memphis

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	consumerDefaultPingInterval = 30 * time.Second
	dlqSubjPrefix               = "$memphis_dlq"
	memphisPmAckSubject         = "$memphis_pm_acks"
)

var (
	ConsumerErrStationUnreachable = errors.New("Station unreachable")
	ConsumerErrConsumeInactive    = errors.New("Consumer is inactive")
)

// Consumer - memphis consumer object.
type Consumer struct {
	Name               string
	ConsumerGroup      string
	PullInterval       time.Duration
	BatchSize          int
	BatchMaxTimeToWait time.Duration
	MaxAckTime         time.Duration
	MaxMsgDeliveries   int
	conn               *Conn
	stationName        string
	subscription       *nats.Subscription
	pingInterval       time.Duration
	subscriptionActive bool
	firstFetch         bool
	consumeActive      bool
	dlqCh              chan *nats.Msg
	consumeQuit        chan struct{}
	pingQuit           chan struct{}
	errHandler         ConsumerErrHandler
}

// Msg - a received message, can be acked.
type Msg struct {
	msg    *nats.Msg
	conn   *Conn
	cgName string
}

type PMsgToAck struct {
	ID       string `json:"id"`
	Sequence string `json:"sequence"`
}

// Msg.Data - get message's data.
func (m *Msg) Data() []byte {
	return m.msg.Data
}

// Msg.Ack - ack the message.
func (m *Msg) Ack() error {
	err := m.msg.Ack()
	if err != nil {
		headers := m.GetHeaders()
		id, ok := headers["$memphis_pm_id"]
		if !ok {
			return err
		} else {
			seq, ok := headers["$memphis_pm_sequence"]
			if !ok {
				return err
			} else {
				msgToAck := PMsgToAck{
					ID:       id,
					Sequence: seq,
				}
				msgToPublish, _ := json.Marshal(msgToAck)
				m.conn.brokerConn.Publish(memphisPmAckSubject, msgToPublish)
			}
		}
	}
	return nil
}

// Msg.GetHeaders - get headers per message
func (m *Msg) GetHeaders() map[string]string {
	headers := map[string]string{}
	for key, value := range m.msg.Header {
		headers[key] = value[0]
	}
	return headers
}

// ConsumerErrHandler is used to process asynchronous errors.
type ConsumerErrHandler func(*Consumer, error)

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

// ConsumerOpts - configuration options for a consumer.
type ConsumerOpts struct {
	Name               string
	StationName        string
	ConsumerGroup      string
	PullInterval       time.Duration
	BatchSize          int
	BatchMaxTimeToWait time.Duration
	MaxAckTime         time.Duration
	MaxMsgDeliveries   int
	GenUniqueSuffix    bool
	ErrHandler         ConsumerErrHandler
}

// getDefaultConsumerOptions - returns default configuration options for consumers.
func getDefaultConsumerOptions() ConsumerOpts {
	return ConsumerOpts{
		PullInterval:       1 * time.Second,
		BatchSize:          10,
		BatchMaxTimeToWait: 5 * time.Second,
		MaxAckTime:         30 * time.Second,
		MaxMsgDeliveries:   10,
		GenUniqueSuffix:    false,
		ErrHandler:         DefaultConsumerErrHandler,
	}
}

// ConsumerOpt  - a function on the options for consumers.
type ConsumerOpt func(*ConsumerOpts) error

// CreateConsumer - creates a consumer.
func (c *Conn) CreateConsumer(stationName, consumerName string, opts ...ConsumerOpt) (*Consumer, error) {
	defaultOpts := getDefaultConsumerOptions()

	defaultOpts.Name = consumerName
	defaultOpts.StationName = stationName
	defaultOpts.ConsumerGroup = consumerName

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, memphisError(err)
			}
		}
	}

	return defaultOpts.createConsumer(c)
}

// ConsumerOpts.createConsumer - creates a consumer using a configuration struct.
func (opts *ConsumerOpts) createConsumer(c *Conn) (*Consumer, error) {
	var err error

	if opts.GenUniqueSuffix {
		opts.Name, err = extendNameWithRandSuffix(opts.Name)
		if err != nil {
			return nil, memphisError(err)
		}
	}

	consumer := Consumer{Name: opts.Name,
		ConsumerGroup:      opts.ConsumerGroup,
		PullInterval:       opts.PullInterval,
		BatchSize:          opts.BatchSize,
		MaxAckTime:         opts.MaxAckTime,
		MaxMsgDeliveries:   opts.MaxMsgDeliveries,
		BatchMaxTimeToWait: opts.BatchMaxTimeToWait,
		conn:               c,
		stationName:        opts.StationName,
		errHandler:         opts.ErrHandler,
	}

	err = c.create(&consumer)
	if err != nil {
		return nil, memphisError(err)
	}

	consumer.firstFetch = true
	consumer.dlqCh = make(chan *nats.Msg, 1)
	consumer.consumeQuit = make(chan struct{})
	consumer.pingQuit = make(chan struct{}, 1)

	consumer.pingInterval = consumerDefaultPingInterval

	subjInternalName := getInternalName(consumer.stationName)
	subj := subjInternalName + ".final"

	durable := getInternalName(consumer.ConsumerGroup)
	consumer.subscription, err = c.brokerPullSubscribe(subj,
		durable,
		nats.ManualAck(),
		nats.MaxRequestExpires(consumer.BatchMaxTimeToWait),
		nats.MaxRequestBatch(opts.BatchSize),
		nats.MaxDeliver(opts.MaxMsgDeliveries))
	if err != nil {
		return nil, memphisError(err)
	}

	consumer.subscriptionActive = true

	go consumer.pingConsumer()

	return &consumer, err
}

// Station.CreateConsumer - creates a producer attached to this station.
func (s *Station) CreateConsumer(name string, opts ...ConsumerOpt) (*Consumer, error) {
	return s.conn.CreateConsumer(s.Name, name, opts...)
}

func DefaultConsumerErrHandler(c *Consumer, err error) {
	log.Printf("Consumer %v: %v", c.Name, err.Error())
}

func (c *Consumer) callErrHandler(err error) {
	if c.errHandler != nil {
		c.errHandler(c, memphisError(err))
	}
}

func (c *Consumer) pingConsumer() {
	ticker := time.NewTicker(c.pingInterval)
	if !c.subscriptionActive {
		log.Fatal("started ping for inactive subscription")
	}

	for {
		select {
		case <-ticker.C:
			_, err := c.subscription.ConsumerInfo()
			if err != nil {
				c.subscriptionActive = false
				c.callErrHandler(ConsumerErrStationUnreachable)
				c.StopConsume()
				return
			}
		case <-c.pingQuit:
			ticker.Stop()
			return
		}
	}

}

// ConsumeHandler - handler for consumed messages
type ConsumeHandler func([]*Msg, error)

// Consumer.Consume - start consuming messages according to the interval configured in the consumer object.
// When a batch is consumed the handlerFunc will be called.
func (c *Consumer) Consume(handlerFunc ConsumeHandler) error {
	go func() {
		if c.firstFetch {
			err := c.firstFetchInit()
			if err != nil {
				handlerFunc(nil, memphisError(err))
				return
			}

			c.firstFetch = false
			msgs, err := c.fetchSubscription()
			handlerFunc(msgs, memphisError(err))
		}

		ticker := time.NewTicker(c.PullInterval)
		defer ticker.Stop()

		for {
			// give first priority to quit signals
			select {
			case <-c.consumeQuit:
				return
			default:
			}

			select {
			case <-ticker.C:
				msgs, err := c.fetchSubscription()

				// ignore fetch timeout if we have messages in the dlq channel
				if err == nats.ErrTimeout && len(c.dlqCh) > 0 {
					err = nil
				}

				// push messages from the dlq channel to the user's handler
				for len(c.dlqCh) > 0 {
					dlqMsg := Msg{msg: <-c.dlqCh, conn: c.conn, cgName: c.ConsumerGroup}
					msgs = append(msgs, &dlqMsg)
				}

				handlerFunc(msgs, memphisError(err))
			case <-c.consumeQuit:
				return
			}
		}
	}()
	c.consumeActive = true
	return nil
}

// StopConsume - stops the continuous consume operation.
func (c *Consumer) StopConsume() {
	if !c.consumeActive {
		c.callErrHandler(ConsumerErrConsumeInactive)
		return
	}
	c.consumeQuit <- struct{}{}
	c.consumeActive = false
}

func (c *Consumer) fetchSubscription() ([]*Msg, error) {
	if !c.subscriptionActive {
		return nil, memphisError(errors.New("station unreachable"))
	}

	subscription := c.subscription
	batchSize := c.BatchSize
	msgs, err := subscription.Fetch(batchSize)
	if err != nil {
		return nil, memphisError(err)
	}

	wrappedMsgs := make([]*Msg, 0, batchSize)

	for _, msg := range msgs {
		wrappedMsgs = append(wrappedMsgs, &Msg{msg: msg, conn: c.conn, cgName: c.ConsumerGroup})
	}
	return wrappedMsgs, nil
}

type fetchResult struct {
	msgs []*Msg
	err  error
}

func (c *Consumer) fetchSubscriprionWithTimeout() ([]*Msg, error) {
	timeoutDuration := c.BatchMaxTimeToWait
	out := make(chan fetchResult, 1)
	go func() {
		msgs, err := c.fetchSubscription()
		out <- fetchResult{msgs: msgs, err: memphisError(err)}
	}()
	select {
	case <-time.After(timeoutDuration):
		return nil, memphisError(errors.New("fetch timed out"))
	case fetchRes := <-out:
		return fetchRes.msgs, memphisError(fetchRes.err)

	}
}

// Fetch - immediately fetch a message batch.
func (c *Consumer) Fetch() ([]*Msg, error) {
	if c.firstFetch {
		err := c.firstFetchInit()
		if err != nil {
			return nil, memphisError(err)
		}

		c.firstFetch = false
	}

	return c.fetchSubscriprionWithTimeout()
}

func (c *Consumer) firstFetchInit() error {
	var err error
	_, err = c.conn.brokerQueueSubscribe(c.getDlqSubjName(), c.getDlqQueueName(), c.createDlqMsgHandler())
	return memphisError(err)
}

func (c *Consumer) createDlqMsgHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		c.dlqCh <- msg
	}
}

func (c *Consumer) getDlqSubjName() string {
	stationName := getInternalName(c.stationName)
	consumerGroup := getInternalName(c.ConsumerGroup)
	return fmt.Sprintf("%v_%v_%v", dlqSubjPrefix, stationName, consumerGroup)
}

func (c *Consumer) getDlqQueueName() string {
	return c.getDlqSubjName()
}

// Destroy - destroy this consumer.
func (c *Consumer) Destroy() error {
	if c.consumeActive {
		c.StopConsume()
	}
	if c.subscriptionActive {
		c.pingQuit <- struct{}{}
	}

	return c.conn.destroy(c)
}

func (c *Consumer) getCreationSubject() string {
	return "$memphis_consumer_creations"
}

func (c *Consumer) getCreationReq() any {
	return createConsumerReq{
		Name:             c.Name,
		StationName:      c.stationName,
		ConnectionId:     c.conn.ConnId,
		ConsumerType:     "application",
		ConsumerGroup:    c.ConsumerGroup,
		MaxAckTimeMillis: int(c.MaxAckTime.Milliseconds()),
		MaxMsgDeliveries: c.MaxMsgDeliveries,
	}
}

func (c *Consumer) handleCreationResp(resp []byte) error {
	return defaultHandleCreationResp(resp)
}

func (c *Consumer) getDestructionSubject() string {
	return "$memphis_consumer_destructions"
}

func (c *Consumer) getDestructionReq() any {
	return removeConsumerReq{Name: c.Name, StationName: c.stationName}
}

// ConsumerName - name for the consumer.
func ConsumerName(name string) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.Name = name
		return nil
	}
}

// StationNameOpt - station name to consume messages from.
func StationNameOpt(stationName string) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.StationName = stationName
		return nil
	}
}

// ConsumerGroup - consumer group name, default is "".
func ConsumerGroup(cg string) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.ConsumerGroup = cg
		return nil
	}
}

// PullInterval - interval between pulls, default is 1 second.
func PullInterval(pullInterval time.Duration) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.PullInterval = pullInterval
		return nil
	}
}

// BatchSize - pull batch size.
func BatchSize(batchSize int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.BatchSize = batchSize
		return nil
	}
}

// BatchMaxWaitTime - max time to wait between pulls, defauls is 5 seconds.
func BatchMaxWaitTime(batchMaxWaitTime time.Duration) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		if batchMaxWaitTime < 1*time.Millisecond {
			batchMaxWaitTime = 1 * time.Millisecond
		}
		opts.BatchMaxTimeToWait = batchMaxWaitTime
		return nil
	}
}

// MaxAckTime - max time for ack a message, in case a message not acked within this time period memphis will resend it.
func MaxAckTime(maxAckTime time.Duration) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.MaxAckTime = maxAckTime
		return nil
	}
}

// MaxMsgDeliveries - max number of message deliveries, by default is 10.
func MaxMsgDeliveries(maxMsgDeliveries int) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.MaxMsgDeliveries = maxMsgDeliveries
		return nil
	}
}

// ConsumerGenUniqueSuffix - whether to generate a unique suffix for this consumer.
func ConsumerGenUniqueSuffix() ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.GenUniqueSuffix = true
		return nil
	}
}

// ConsumerErrorHandler - handler for consumer errors.
func ConsumerErrorHandler(ceh ConsumerErrHandler) ConsumerOpt {
	return func(opts *ConsumerOpts) error {
		opts.ErrHandler = ceh
		return nil
	}
}
