package memphis

import (
	"fmt"
	"net"
	"strconv"
	"encoding/json"
)

// Option is a function on the options for a connection.
type Option func(*Options) error

type Options struct {
	Host string
	ManagementPort int
	TcpPort int
	DataPort int
	Username string
	ConnectionToken string
	Reconnect bool
	MaxReconnect int
	ReconnectIntervalMilis int
	TimeoutMilis int
}

type Conn struct {
	connected bool
	opts Options
	ConnId string
}


func GetDefaultOptions() Options {
	return Options{
		ManagementPort: 5555,
		TcpPort: 6666,
		DataPort: 7766,
		Username: "",
		ConnectionToken: "",
		Reconnect: true,
		MaxReconnect: 3,
		ReconnectIntervalMilis: 200,
		TimeoutMilis: 15000,
	}
}

type connectReq struct {
	Username string `json:"username"`
	ConnToken string `json:"broker_creds"`
	ConnId string `json:"connection_id"`
}

type connectResp struct {
	ConnId string `json:"connection_id"`
	AccessToken string `json:"access_token"`
	AccessTokenExpiry int `json:"access_token_exp"`
	PingInterval int `json:"ping_interval_ms"`
}

func Connect(host string, options ...Option) (*Conn, error) {
	opts := GetDefaultOptions()

	opts.Host = host

	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	return opts.Connect()
}


func (opts Options) Connect() (*Conn, error) {
	c := Conn{
		connected: false,
		opts: opts,
	}

	// connect to TcpPort using username, token and connectionID
	address := opts.Host + ":" + strconv.Itoa(opts.TcpPort)
	tcpConn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	connectMsg, err := json.Marshal(connectReq{
		Username: opts.Username,
		ConnToken: opts.ConnectionToken,
		ConnId: "",
	})
	if err != nil {
		return nil, err
	}

	_, err = tcpConn.Write(connectMsg)
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1024)
	mLen, err := tcpConn.Read(b)
	if err != nil {
		return nil, err
	}

	var resp connectResp
	json.Unmarshal(b[:mLen], &resp)
	fmt.Println("Received:", resp)

	c.ConnId = resp.ConnId

	return &c, nil
}

// func Producer() {}
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
		o.ReconnectIntervalMilis = reconnectInterval
		return nil
	}
}


func TimeoutMilis(timeout int) Option {
	return func(o *Options) error {
		o.TimeoutMilis = timeout
		return nil
	}
}
