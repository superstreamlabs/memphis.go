package memphis

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"
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
	pingQuitChan         chan struct{}
	refreshTokenQuitChan chan struct{}
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
		opts:      opts,
	}

	// connect to TcpPort using username, token and connectionID
	address := opts.Host + ":" + strconv.Itoa(opts.TcpPort)
	tcpConn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	connectMsg, err := json.Marshal(connectReq{
		Username:  opts.Username,
		ConnToken: opts.ConnectionToken,
		ConnId:    "",
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
	c.AccessToken = resp.AccessToken

	if resp.AccessTokenExpiry != 0 {
		refreshReq, err := json.Marshal(refreshAccessTokenReq{
			ResendAccessToken: true,
		})
		if err != nil {
			return nil, err
		}
		c.refreshTokenQuitChan = heartBeat(tcpConn, resp.AccessTokenExpiry, refreshReq)
	}

	if resp.PingInterval != 0 {
		ping, err := json.Marshal(pingReq{
			Ping: true,
		})
		if err != nil {
			return nil, err
		}
		c.pingQuitChan = heartBeat(tcpConn, resp.PingInterval, ping)
	}

	return &c, nil
}

func heartBeat(tcpConn net.Conn, interval int, msg []byte) chan struct{} {
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Sending:", string(msg))
				for n, err := tcpConn.Write(msg); n < len(msg); {
					if err != nil {
						panic(err)
					}
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
