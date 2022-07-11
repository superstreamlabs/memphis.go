package memphis

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

const (
	ConnectDefaultTcpCheckInterval = 2 * time.Second
)

// Option is a function on the options for a connection.
type Option func(*Options) error

type Options struct {
	Host              string
	ManagementPort    int
	TcpPort           int
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

type connState struct {
	tcpConnected         chan bool
	dataConnected        chan bool
	queryConnection      chan queryReq
	connectCheckQuitChan chan struct{}
	pingQuitChan         chan struct{}
	refreshTokenQuitChan chan struct{}
	die                  chan struct{}
}

func (c *Conn) IsConnected() bool {
	query := queryReq{
		resp: make(chan bool),
	}
	c.state.queryConnection <- query
	return <-query.resp
}

// Conn - holds the connection with memphis.
type Conn struct {
	opts             Options
	ConnId           string
	accessToken      string
	state            connState
	tcpConn          net.Conn
	tcpConnLock      sync.Mutex
	refreshTokenWait time.Duration
	pingWait         time.Duration
	brokerConn       *nats.Conn
	js               nats.JetStreamContext
}

// GetDefaultOptions - returns default configuration options for the client.
func GetDefaultOptions() Options {
	return Options{
		ManagementPort:    5555,
		TcpPort:           6666,
		DataPort:          7766,
		Reconnect:         true,
		MaxReconnect:      3,
		ReconnectInterval: 200 * time.Millisecond,
		Timeout:           15 * time.Second,
	}
}

type connectReq struct {
	Username  string `json:"username"`
	ConnToken string `json:"broker_creds"`
	ConnId    string `json:"connection_id"`
}

type connectResp struct {
	ConnId            string `json:"connection_id"`
	AccessToken       string `json:"access_token"`
	AccessTokenExpiry int    `json:"access_token_exp"`
	PingInterval      int    `json:"ping_interval_ms"`
}

type refreshTokenResp connectResp

type refreshAccessTokenReq struct {
	ResendAccessToken bool `json:"resend_access_token"`
}

type pingReq struct {
	Ping bool `json:"ping"`
}

type errorResp struct {
	Message string `json:"message"`
}

// Connect - creates connection with memphis.
func Connect(host, username, connectionToken string, options ...Option) (*Conn, error) {
	opts := GetDefaultOptions()

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

	return opts.Connect()
}

func normalizeHost(host string) string {
	r := regexp.MustCompile("^http(s?)://")
	return r.ReplaceAllString(host, "")
}

func (opts Options) Connect() (*Conn, error) {
	if opts.MaxReconnect > 9 {
		opts.MaxReconnect = 9
	}

	if !opts.Reconnect {
		opts.MaxReconnect = 0
	}

	c := Conn{
		opts: opts,
	}

	c.state.tcpConnected = make(chan bool)
	c.state.dataConnected = make(chan bool)
	c.state.queryConnection = make(chan queryReq)
	c.state.pingQuitChan = make(chan struct{})
	c.state.connectCheckQuitChan = make(chan struct{})
	c.state.refreshTokenQuitChan = make(chan struct{})
	c.state.die = make(chan struct{})

	go listenForConnChanges(&c)

	firstAttempt := true
	if err := c.startTcpConn(firstAttempt); err != nil {
		return nil, err
	}

	if err := c.startDataConn(); err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Conn) doReconnect() error {
	c.tcpConn.Close()
	firstAttempt := false
	err := c.startTcpConn(firstAttempt)
	return err
}

func listenForConnChanges(c *Conn) {
	cs := &c.state
	tcpConnected, dataConnected, timedOpsStarted := false, false, false
	for {
		select {
		case req := <-cs.queryConnection:
			req.resp <- tcpConnected && dataConnected

		case tcpConnected = <-cs.tcpConnected:
			if tcpConnected {
				c.checkTcpConnection()
				c.refreshToken()
				c.sendPing()
				timedOpsStarted = true
			} else {
				if timedOpsStarted {
					c.stopTimedOps()
				}
				err := c.doReconnect()
				if err != nil {
					log.Error("reconnection failed")
					c.closeExceptConnListener()
					dataConnected = false
				}
			}

		case dataConnected = <-cs.dataConnected:
			if !dataConnected {
				log.Warning("broker conn disconnected")
				c.closeExceptConnListener()
				tcpConnected = false
			}
		case <-cs.die:
			return
		}
	}
}

func (c *Conn) dial(resp *connectResp) error {
	opts := &c.opts
	url := opts.Host + ":" + strconv.Itoa(opts.TcpPort)
	var err error

	c.tcpConn, err = net.Dial("tcp", url)
	if err != nil {
		return err
	}

	connectMsg, err := json.Marshal(connectReq{
		Username:  opts.Username,
		ConnToken: opts.ConnectionToken,
		ConnId:    "",
	})
	if err != nil {
		c.tcpConn.Close()
		return err
	}

	b, err := c.tcpRequestResponse(connectMsg)
	if err != nil {
		c.tcpConn.Close()
		return err
	}

	err = json.Unmarshal(b, &resp)
	if err != nil {
		c.tcpConn.Close()
		return err
	}

	return nil
}

func (c *Conn) startTcpConn(firstAttempt bool) error {
	opts := &c.opts

	log.Debug("tcp connection attempt started")

	connAttempts := opts.MaxReconnect
	if firstAttempt {
		connAttempts += 1
	}

	var err error
	var resp connectResp
	reconnectWaitChan := make(chan struct{}, 1)
	for i := 0; i < connAttempts; i++ {
		go func() {
			<-time.After(c.opts.ReconnectInterval)
			reconnectWaitChan <- struct{}{}
		}()
		err = c.dial(&resp)
		if err != nil {
			<-reconnectWaitChan
			continue
		}
		break
	}

	if err != nil {
		return err
	}

	log.Debug("tcp connection attempt finished successfully")

	c.ConnId = resp.ConnId
	c.accessToken = resp.AccessToken
	c.refreshTokenWait = time.Duration(resp.AccessTokenExpiry) * time.Millisecond
	c.pingWait = time.Duration(resp.PingInterval) * time.Millisecond

	c.state.tcpConnected <- true
	return nil
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

func (c *Conn) checkTcpConnection() {
	ticker := time.NewTicker(ConnectDefaultTcpCheckInterval)

	buff := make([]byte, 1)
	go func() {
		for {
			log.Debug("tcp connection check iteration")

			select {
			case <-ticker.C:
				c.tcpConnLock.Lock()

				if err := c.tcpConn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
					log.Error("failed setting deadline for check read, connection monitoring may not work")
				}

				_, err := c.tcpConn.Read(buff)
				if err == io.EOF {
					c.state.tcpConnected <- false
				}

				if err := c.tcpConn.SetReadDeadline(time.Time{}); err != nil {
					log.Error("failed setting deadline for check read, connection monitoring may not work")
				}
				c.tcpConnLock.Unlock()
			case <-c.state.connectCheckQuitChan:
				ticker.Stop()
				return
			}
		}
	}()
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
		DisconnectedErrCB: c.createBrokerDisconnectionHandler(),
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

	c.state.dataConnected <- true
	return nil
}

func (c *Conn) createBrokerDisconnectionHandler() nats.ConnErrHandler {
	return func(_ *nats.Conn, err error) {
		c.state.dataConnected <- false
	}
}

func (c *Conn) stopTimedOps() {
	if c.pingWait != 0 {
		c.state.pingQuitChan <- struct{}{}
	}

	if c.refreshTokenWait != 0 {
		c.state.refreshTokenQuitChan <- struct{}{}
	}

	c.state.connectCheckQuitChan <- struct{}{}
}

func (c *Conn) Close() {
	c.closeExceptConnListener()
	c.state.die <- struct{}{}
}

func (c *Conn) closeExceptConnListener() {
	c.tcpConn.Close()
	c.brokerConn.Close()
}

func (c *Conn) refreshToken() {
	if c.refreshTokenWait == 0 {
		return
	}

	refreshReq, err := json.Marshal(refreshAccessTokenReq{
		ResendAccessToken: true,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		wait := c.refreshTokenWait
		for {
			select {
			case <-time.After(wait):
				b, err := c.tcpRequestResponse(refreshReq)
				if err != nil {
					log.Error("Failed requesting refresh token")
					return
				}

				var resp refreshTokenResp
				err = json.Unmarshal(b, &resp)
				if err != nil {
					log.Error("Failed parsing refresh token response")
					return
				}

				c.accessToken = resp.AccessToken
				wait = time.Duration(resp.AccessTokenExpiry) * time.Millisecond

			case <-c.state.refreshTokenQuitChan:
				return
			}
		}
	}()
}

func (c *Conn) sendPing() {
	if c.pingWait == 0 {
		return
	}

	pingReq, err := json.Marshal(pingReq{
		Ping: true,
	})
	if err != nil {
		panic(err)
	}

	go func() {
		wait := c.pingWait
		for {
			select {
			case <-time.After(wait):
				b, err := c.tcpRequestResponse(pingReq)
				if err != nil {
					log.Error("Failed requesting ping")
					return
				}

				var resp refreshTokenResp
				err = json.Unmarshal(b, &resp)
				if err != nil {
					log.Error("Failed parsing ping response")
					return
				}
				wait = time.Duration(resp.AccessTokenExpiry) * time.Millisecond
			case <-c.state.pingQuitChan:
				return
			}
		}
	}()
}

func (c *Conn) mgmtRequest(apiMethod string, apiPath string, reqStruct any) error {
	if !c.IsConnected() {
		return errors.New("Connection object is disconnected")
	}

	managementPort := strconv.Itoa(c.opts.ManagementPort)
	url := "http://" + c.opts.Host + ":" + managementPort + apiPath
	reqJson, err := json.Marshal(reqStruct)
	if err != nil {
		return err
	}

	reqBody := bytes.NewBuffer(reqJson)

	req, err := http.NewRequest(apiMethod, url, reqBody)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", "Bearer "+c.accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 { //HTTP success status code
		var errorResp errorResp
		err = json.Unmarshal(respBody, &errorResp)
		if err != nil {
			return err
		}
		return errors.New(errorResp.Message)
	}

	return nil
}

func (c *Conn) brokerPublish(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return c.js.PublishMsgAsync(msg, opts...)
}

func (c *Conn) brokerSubscribe(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return c.js.PullSubscribe(subject, durable, opts...)
}

// ManagementPort - default is 5555.
func ManagementPort(port int) Option {
	return func(o *Options) error {
		o.ManagementPort = port
		return nil
	}
}

// TcpPort - default is 6666.
func TcpPort(port int) Option {
	return func(o *Options) error {
		o.TcpPort = port
		return nil
	}
}

// DataPort - default is 7766.
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

type apiObj interface {
	getCreationApiPath() string
	getCreationReq() any

	getDestructionApiPath() string
	getDestructionReq() any
}

func (c *Conn) create(o apiObj) error {
	apiPath := o.getCreationApiPath()
	creationReq := o.getCreationReq()

	return c.mgmtRequest("POST", apiPath, creationReq)
}

func (c *Conn) destroy(o apiObj) error {
	apiPath := o.getDestructionApiPath()
	destructionReq := o.getDestructionReq()

	return c.mgmtRequest("DELETE", apiPath, destructionReq)
}
