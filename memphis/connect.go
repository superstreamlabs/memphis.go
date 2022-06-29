package memphis

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
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

type Conn struct {
	connected            bool
	opts                 Options
	ConnId               string
	AccessToken          string
	tcpConn              net.Conn
	pingQuitChan         chan bool
	refreshTokenQuitChan chan bool
	brokerManager        *nats.Conn
	brokerConn           nats.JetStream
}

func GetDefaultOptions() Options {
	return Options{
		ManagementPort:          5555,
		TcpPort:                 6666,
		DataPort:                7766,
		Username:                "",
		ConnectionToken:         "",
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

type refreshAccessTokenReq struct {
	ResendAccessToken bool `json:"resend_access_token"`
}

type pingReq struct {
	Ping bool `json:"ping"`
}

func Connect(host string, options ...Option) (*Conn, error) {
	opts := GetDefaultOptions()

	opts.Host = normalizeHost(host)

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
		connected: false,
		opts:      opts,
	}

	// connect to TcpPort using username, token and connectionID
	err := c.setupTcpConn()
	if err != nil {
		return nil, err
	}

	err = c.setupDataConn()
	if err != nil {
		return nil, err
	}

	c.connected = true
	return &c, nil
}

func (c *Conn) setupTcpConn() error {
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
		return err
	}

	_, err = c.tcpConn.Write(connectMsg)
	if err != nil {
		return err
	}

	b := make([]byte, 1024)
	mLen, err := c.tcpConn.Read(b)
	if err != nil {
		return err
	}

	var resp connectResp
	err = json.Unmarshal(b[:mLen], &resp)
	if err != nil {
		return err
	}

	c.ConnId = resp.ConnId
	c.AccessToken = resp.AccessToken

	if resp.AccessTokenExpiry != 0 {
		refreshReq, err := json.Marshal(refreshAccessTokenReq{
			ResendAccessToken: true,
		})
		if err != nil {
			return err
		}
		c.refreshTokenQuitChan = heartBeat(c.tcpConn, resp.AccessTokenExpiry, refreshReq)
	}

	if resp.PingInterval != 0 {
		ping, err := json.Marshal(pingReq{
			Ping: true,
		})
		if err != nil {
			return err
		}
		c.pingQuitChan = heartBeat(c.tcpConn, resp.PingInterval, ping)
	}

	return nil
}

func (c *Conn) setupDataConn() error {
	opts := &c.opts

	var err error
	url := opts.Host + ":" + strconv.Itoa(opts.DataPort)
	natsOpts := nats.Options{
		Url:            url,
		AllowReconnect: opts.Reconnect,
		MaxReconnect:   opts.MaxReconnect,
		ReconnectWait:  time.Duration(opts.ReconnectIntervalMillis) * time.Millisecond,
		Timeout:        time.Duration(opts.TimeoutMillis) * time.Millisecond,
		Token:          opts.ConnectionToken,
	}
	c.brokerManager, err = natsOpts.Connect()

	if err != nil {
		fmt.Print(err.Error())
		return err
	}
	c.brokerConn, err = c.brokerManager.JetStream()

	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

func (c *Conn) Close() {
	c.refreshTokenQuitChan <- true
	c.pingQuitChan <- true

	c.tcpConn.Close()
	c.brokerManager.Close()
	c.connected = false
}

func heartBeat(tcpConn net.Conn, interval int, msg []byte) chan bool {
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	quit := make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Sending:", string(msg))
				_, err := tcpConn.Write(msg)
				if err != nil {
					panic(err)
				}

				fmt.Println("Sent:", string(msg))

				b := make([]byte, 1024)
				mLen, err := tcpConn.Read(b)
				if err != nil {
					fmt.Println("error received")
				}
				fmt.Println("Received:", string(b[:mLen]))

			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	return quit
}

func (c *Conn) mgmtRequest(apiMethod string, apiPath string, reqStruct any) error {
	if !c.connected {
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

	req.Header.Add("Authorization", "Bearer "+c.AccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	fmt.Println("response", string(respBody))
	if resp.StatusCode != 200 {
		fmt.Println(resp.StatusCode)
		return errors.New("bad response")
	}

	return nil
}

func (c *Conn) brokerPublish(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	return c.brokerConn.PublishMsgAsync(msg, opts...)
}

func (c *Conn) brokerSubscribe(subject, durable string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return c.brokerConn.PullSubscribe(subject, durable, opts...)
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

func Username(username string) Option {
	return func(o *Options) error {
		o.Username = username
		return nil
	}
}

func ConnectionToken(token string) Option {
	return func(o *Options) error {
		o.ConnectionToken = token
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
	fmt.Printf("Creation of %v, req %v", o, creationReq)

	return c.mgmtRequest("POST", apiPath, creationReq)
}

func (c *Conn) destroy(o apiObj) error {
	apiPath := o.getDestructionApiPath()
	destructionReq := o.getDestructionReq()

	return c.mgmtRequest("DELETE", apiPath, destructionReq)
}
