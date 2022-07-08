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

// Option is a function on the options for a connection.
type Option func(*Options) error

type Options struct {
	Host                    string
	ManagementPort          int
	TcpPort                 int
	DataPort                int
	Username                string
	ConnectionToken         string
	Reconnect               bool
	MaxReconnect            int
	ReconnectIntervalMillis int
	TimeoutMillis           int
}

type queryReq struct {
	resp chan bool
}

type ConnState struct {
	reconnectAttemptsLeft int
	tcpConnected          chan bool
	dataConnected         chan bool
	queryConnection       chan queryReq
	connectCheckQuitChan  chan struct{}
	pingQuitChan          chan struct{}
	refreshTokenQuitChan  chan struct{}
	die                   chan struct{}
}

func (c *Conn) IsConnected() bool {
	query := queryReq{
		resp: make(chan bool),
	}
	c.state.queryConnection <- query
	return <-query.resp
}

type Conn struct {
	opts             Options
	ConnId           string
	accessToken      string
	state            ConnState
	tcpConn          net.Conn
	tcpConnLock      sync.Mutex
	refreshTokenWait time.Duration
	pingWait         time.Duration
	brokerConn       *nats.Conn
	js               nats.JetStream
}

func GetDefaultOptions() Options {
	return Options{
		ManagementPort:          5555,
		TcpPort:                 6666,
		DataPort:                7766,
		Reconnect:               true,
		MaxReconnect:            3,
		ReconnectIntervalMillis: 200,
		TimeoutMillis:           15000,
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
	c := Conn{
		opts: opts,
	}

	if opts.MaxReconnect > 9 {
		opts.MaxReconnect = 9
	}
	c.state.reconnectAttemptsLeft = opts.MaxReconnect

	if !opts.Reconnect {
		c.state.reconnectAttemptsLeft = 0
	}

	c.state.tcpConnected = make(chan bool)
	c.state.dataConnected = make(chan bool)
	c.state.queryConnection = make(chan queryReq)
	c.state.pingQuitChan = make(chan struct{})
	c.state.connectCheckQuitChan = make(chan struct{})
	c.state.refreshTokenQuitChan = make(chan struct{})
	c.state.die = make(chan struct{})

	go listenForConnChanges(&c)

	var err error
	err = c.setupTcpConn()
	if err != nil {
		return nil, err
	}

	err = c.setupDataConn()
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func listenForConnChanges(c *Conn) {
	cs := &c.state
	tcpConnected, dataConnected := false, false
	for {
		select {
		case req := <-cs.queryConnection:
			req.resp <- tcpConnected && dataConnected

		case tcpConnected = <-cs.tcpConnected:
			if tcpConnected {
				go c.checkTcpConnection()
			} else {
				log.Warning("TCP conn disconnected")
				c.closeTcpConn()
				if cs.reconnectAttemptsLeft > 0 {
					log.Warning("reconnection attempt for TCP conn")
					cs.reconnectAttemptsLeft--
					go c.setupTcpConn()
				} else {
					go c.Close()
				}
			}

		case dataConnected = <-cs.dataConnected:
			if !dataConnected {
				log.Warning("Broker conn disconnected")
				go c.Close()
			}
		case <-cs.die:
			return
		}
	}
}

func (c *Conn) setupTcpConn() error {
	opts := &c.opts
	url := opts.Host + ":" + strconv.Itoa(opts.TcpPort)

	log.Debug("connecting TCP")

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
		return err
	}

	b, err := c.tcpRequestResponse(connectMsg)

	var resp connectResp
	err = json.Unmarshal(b, &resp)
	if err != nil {
		return err
	}

	c.ConnId = resp.ConnId
	c.accessToken = resp.AccessToken
	c.refreshTokenWait = time.Duration(resp.AccessTokenExpiry) * time.Millisecond
	c.pingWait = time.Duration(resp.PingInterval) * time.Millisecond
	refreshToken(c)
	sendPing(c)

	log.Debug("Finished TCP conn setup")
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
	ticker := time.NewTicker(2 * time.Second)

	buff := make([]byte, 1)
	for {
		log.Debug("Connection check iteration")

		select {
		case <-ticker.C:
			c.tcpConnLock.Lock()

			c.tcpConn.SetReadDeadline(time.Now().Add(1 * time.Second))

			_, err := c.tcpConn.Read(buff)
			if err == io.EOF {
				c.state.tcpConnected <- false
			}
			c.tcpConn.SetReadDeadline(time.Time{})
			c.tcpConnLock.Unlock()
		case <-c.state.connectCheckQuitChan:
			ticker.Stop()
			return
		}
	}
}

func (c *Conn) setupDataConn() error {
	opts := &c.opts

	var err error
	url := opts.Host + ":" + strconv.Itoa(opts.DataPort)
	natsOpts := nats.Options{
		Url:               url,
		AllowReconnect:    opts.Reconnect,
		MaxReconnect:      opts.MaxReconnect,
		ReconnectWait:     time.Duration(opts.ReconnectIntervalMillis) * time.Millisecond,
		Timeout:           time.Duration(opts.TimeoutMillis) * time.Millisecond,
		Token:             opts.ConnectionToken,
		DisconnectedErrCB: c.createBrokerDisconnectionHandler(),
	}
	c.brokerConn, err = natsOpts.Connect()

	if err != nil {
		log.Error(err.Error())
		return err
	}
	c.js, err = c.brokerConn.JetStream()

	if err != nil {
		log.Error(err.Error())
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

func (c *Conn) closeTcpConn() {
	if c.pingWait != 0 {
		c.state.pingQuitChan <- struct{}{}
	}

	if c.refreshTokenWait != 0 {
		c.state.refreshTokenQuitChan <- struct{}{}
	}

	c.state.connectCheckQuitChan <- struct{}{}

	c.tcpConn.Close()
}

func (c *Conn) Close() {
	c.closeTcpConn()
	c.brokerConn.Close()
	c.state.die <- struct{}{}
}

func refreshToken(c *Conn) {
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

func sendPing(c *Conn) {
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

func ManagementPort(port int) Option {
	return func(o *Options) error {
		o.ManagementPort = port
		return nil
	}
}

func TcpPort(port int) Option {
	return func(o *Options) error {
		o.TcpPort = port
		return nil
	}
}

func DataPort(port int) Option {
	return func(o *Options) error {
		o.DataPort = port
		return nil
	}
}

func Reconnect(reconnect bool) Option {
	return func(o *Options) error {
		o.Reconnect = reconnect
		return nil
	}
}

func MaxReconnect(maxReconnect int) Option {
	return func(o *Options) error {
		o.MaxReconnect = maxReconnect
		return nil
	}
}

func ReconnectIntervalMilis(reconnectInterval int) Option {
	return func(o *Options) error {
		o.ReconnectIntervalMillis = reconnectInterval
		return nil
	}
}

func TimeoutMillis(timeout int) Option {
	return func(o *Options) error {
		o.TimeoutMillis = timeout
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
