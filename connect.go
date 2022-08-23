// Copyright 2021-2022 The Memphis Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memphis

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

// Option is a function on the options for a connection.
type Option func(*Options) error

type Options struct {
	Host              string
	Port              int
	Username          string
	ConnectionToken   string
	Reconnect         bool
	MaxReconnect      int
	ReconnectInterval time.Duration
	Timeout           time.Duration
}

type queryReq struct {
	resp chan bool
}

func (c *Conn) IsConnected() bool {
	return c.brokerConn.IsConnected()
}

// Conn - holds the connection with memphis.
type Conn struct {
	opts       Options
	ConnId     string
	username   string
	brokerConn *nats.Conn
	js         nats.JetStreamContext
}

// getDefaultOptions - returns default configuration options for the client.
func getDefaultOptions() Options {
	return Options{
		Port:              6666,
		Reconnect:         true,
		MaxReconnect:      3,
		ReconnectInterval: 200 * time.Millisecond,
		Timeout:           15 * time.Second,
	}
}

type errorResp struct {
	Message string `json:"message"`
}

// Connect - creates connection with memphis.
func Connect(host, username, connectionToken string, options ...Option) (*Conn, error) {
	opts := getDefaultOptions()

	opts.Host = normalizeHost(host)
	opts.Username = username
	opts.ConnectionToken = connectionToken

	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	return opts.connect()
}

func normalizeHost(host string) string {
	r := regexp.MustCompile("^http(s?)://")
	return r.ReplaceAllString(host, "")
}

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
	  return "", err
	}
	return hex.EncodeToString(bytes), nil
  }

func (opts Options) connect() (*Conn, error) {
	if opts.MaxReconnect > 9 {
		opts.MaxReconnect = 9
	}

	if !opts.Reconnect {
		opts.MaxReconnect = 0
	}

	connId, err := randomHex(24)
	if err != nil {
		return nil, err
	}

	c := Conn{
		ConnId: connId,
		opts: opts,
	}

	if err := c.startConn(); err != nil {
		return nil, err
	}

	return &c, nil
}

func disconnectedError(conn *nats.Conn, err error) {
	fmt.Printf("Error %v", err.Error())
}

func (c *Conn) startConn() error {
	opts := &c.opts

	var err error
	url := opts.Host + ":" + strconv.Itoa(opts.Port)
	natsOpts := nats.Options{
		Url:               url,
		AllowReconnect:    opts.Reconnect,
		MaxReconnect:      opts.MaxReconnect,
		ReconnectWait:     opts.ReconnectInterval,
		Timeout:           opts.Timeout,
		Token:             opts.ConnectionToken,
		DisconnectedErrCB: disconnectedError,
		Name:              c.ConnId + "::" + opts.Username,
	}
	c.brokerConn, err = natsOpts.Connect()

	if err != nil {
		return err
	}
	c.js, err = c.brokerConn.JetStream()

	if err != nil {
		c.brokerConn.Close()
		return err
	}
	c.username = opts.Username
	return nil
}

func (c *Conn) Close() {
	c.brokerConn.Close()
}

func (c *Conn) brokerCorePublish(subject, reply string, msg []byte) error {
	return c.brokerConn.PublishRequest(subject, reply, msg)
}

func (c *Conn) brokerPublish(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return c.js.PublishMsgAsync(msg, opts...)
}

func (c *Conn) brokerPullSubscribe(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return c.js.PullSubscribe(subject, durable, opts...)
}

func (c *Conn) brokerQueueSubscribe(subj, queue string, cb nats.MsgHandler) (*nats.Subscription, error) {
	return c.brokerConn.QueueSubscribe(subj, queue, cb)
}

// Port - default is 6666.
func Port(port int) Option {
	return func(o *Options) error {
		o.Port = port
		return nil
	}
}

// Reconnect - whether to do reconnect while connection is lost.
func Reconnect(reconnect bool) Option {
	return func(o *Options) error {
		o.Reconnect = reconnect
		return nil
	}
}

// MaxReconnect - the amount of reconnect attempts.
func MaxReconnect(maxReconnect int) Option {
	return func(o *Options) error {
		o.MaxReconnect = maxReconnect
		return nil
	}
}

// ReconnectInterval - interval in miliseconds between reconnect attempts.
func ReconnectInterval(reconnectInterval time.Duration) Option {
	return func(o *Options) error {
		o.ReconnectInterval = reconnectInterval
		return nil
	}
}

// Timeout - connection timeout in miliseconds.
func Timeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.Timeout = timeout
		return nil
	}
}

type directObj interface {
	getCreationSubject() string
	getCreationReq() any

	getDestructionSubject() string
	getDestructionReq() any
}

func (c *Conn) create(do directObj) error {
	subject := do.getCreationSubject()
	creationReq := do.getCreationReq()

	b, err := json.Marshal(creationReq)
	if err != nil {
		return err
	}

	msg, err := c.brokerConn.Request(subject, b, 1*time.Second)
	if err != nil {
		return err
	}
	if len(msg.Data) > 0 {
		return errors.New(string(msg.Data))
	}

	return nil
}

func (c *Conn) destroy(o directObj) error {
	subject := o.getDestructionSubject()
	destructionReq := o.getDestructionReq()

	b, err := json.Marshal(destructionReq)
	if err != nil {
		return err
	}

	msg, err := c.brokerConn.Request(subject, b, 1*time.Second)
	if err != nil {
		return err
	}
	if len(msg.Data) > 0 {
		return errors.New(string(msg.Data))
	}

	return nil
}
