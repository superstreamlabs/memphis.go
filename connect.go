// Copyright 2021-2022 The Memphis Authors
// Licensed under the MIT License (the "License");
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// This license limiting reselling the software itself "AS IS".
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package memphis

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/memphisdev/memphis-nats.go"
)

const (
	ConnectDefaultTcpCheckInterval = 2 * time.Second
	// TODO (or/shoham) use userType for requests that need userType
	// (look in the server for handlers that use getUserDetailsFromMiddleware and later use user.userType
	// e.g. CreateConsumer)
	userType = "application"
)

// Option is a function on the options for a connection.
type Option func(*Options) error

type Options struct {
	Host              string
	ManagementPort    int
	DataPort          int
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
	opts             Options
	ConnId           string
	username         string
	userType         string
	tcpConn          net.Conn
	tcpConnLock      sync.Mutex
	refreshTokenWait time.Duration
	pingWait         time.Duration
	brokerConn       *nats.Conn
	js               nats.JetStreamContext
}

// getDefaultOptions - returns default configuration options for the client.
func getDefaultOptions() Options {
	return Options{
		ManagementPort:    5555,
		DataPort:          6666,
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

func (opts Options) connect() (*Conn, error) {
	if opts.MaxReconnect > 9 {
		opts.MaxReconnect = 9
	}

	if !opts.Reconnect {
		opts.MaxReconnect = 0
	}

	c := Conn{
		opts: opts,
	}

	if err := c.startDataConn(); err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Conn) tcpRequestResponse(req []byte) ([]byte, error) {
	c.tcpConnLock.Lock()
	_, err := c.tcpConn.Write(req)
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1024)
	bLen, err := c.tcpConn.Read(b)
	c.tcpConnLock.Unlock()

	if err != nil {
		return nil, err
	}
	return b[:bLen], nil
}

func disconnectedError(conn *nats.Conn, err error) {
	fmt.Printf("Error %v", err.Error())
}

func (c *Conn) startDataConn() error {
	opts := &c.opts

	var err error
	url := opts.Host + ":" + strconv.Itoa(opts.DataPort)
	natsOpts := nats.Options{
		Url:               url,
		AllowReconnect:    opts.Reconnect,
		MaxReconnect:      opts.MaxReconnect,
		ReconnectWait:     opts.ReconnectInterval,
		Timeout:           opts.Timeout,
		Token:             opts.ConnectionToken,
		User:              opts.Username,
		DisconnectedErrCB: disconnectedError,
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
	c.ConnId, err = c.brokerConn.GetConnectionId(3 * time.Second)
	if err != nil {
		return err
	}

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

// ManagementPort - default is 5555.
func ManagementPort(port int) Option {
	return func(o *Options) error {
		o.ManagementPort = port
		return nil
	}
}

// DataPort - default is 6666.
func DataPort(port int) Option {
	return func(o *Options) error {
		o.DataPort = port
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
