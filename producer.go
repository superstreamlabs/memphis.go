package memphis

import (
	"time"

	"github.com/nats-io/nats.go"
)

// Producer - memphis producer object.
type Producer struct {
	Name        string
	stationName string
	conn        *Conn
}

type createProducerReq struct {
	Name         string `json:"name"`
	StationName  string `json:"station_name"`
	ConnectionId string `json:"connection_id"`
	ProducerType string `json:"producer_type"`
}

type removeProducerReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
}

// CreateProducer - creates a producer.
func (c *Conn) CreateProducer(stationName, name string) (*Producer, error) {
	p := Producer{Name: name, stationName: stationName, conn: c}
	return &p, c.create(&p)
}

// Station.CreateProducer - creates a producer attached to this station.
func (s *Station) CreateProducer(name string) (*Producer, error) {
	return s.conn.CreateProducer(s.Name, name)
}

func (p *Producer) getCreationApiPath() string {
	return "/api/producers/createProducer"
}

func (p *Producer) getCreationReq() any {
	return createProducerReq{
		Name:         p.Name,
		StationName:  p.stationName,
		ConnectionId: p.conn.ConnId,
		ProducerType: "application",
	}
}

func (p *Producer) getDestructionApiPath() string {
	return "/api/producers/destroyProducer"
}

func (p *Producer) getDestructionReq() any {
	return removeProducerReq{Name: p.Name, StationName: p.stationName}
}

// Destroy - destoy this producer.
func (p *Producer) Destroy() error {
	return p.conn.destroy(p)
}

// ProduceOpts - configuration options for produce operations.
type ProduceOpts struct {
	Message    []byte
	AckWaitSec int
}

// ProduceOpt - a function on the options for produce operations.
type ProduceOpt func(*ProduceOpts) error

// GetDefaultProduceOpts - returns default configuration options for produce operations.
func GetDefaultProduceOpts() ProduceOpts {
	return ProduceOpts{AckWaitSec: 15}
}

// Producer.Produce - produces a message into a station.
func (p *Producer) Produce(message []byte, opts ...ProduceOpt) error {
	defaultOpts := GetDefaultProduceOpts()

	defaultOpts.Message = message

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return err
			}
		}
	}

	return defaultOpts.Produce(p)

}

// ProducerOpts.Produce - produces a message into a station using a configuration struct.
func (opts *ProduceOpts) Produce(p *Producer) error {
	natsMessage := nats.Msg{
		Header:  map[string][]string{"connectionId": {p.conn.ConnId}, "producedBy": {p.Name}},
		Subject: p.stationName + ".final",
		Data:    opts.Message,
	}

	stallWaitDuration := time.Second * time.Duration(opts.AckWaitSec)
	paf, err := p.conn.brokerPublish(&natsMessage, nats.StallWait(stallWaitDuration))
	if err != nil {
		return err
	}

	select {
	case <-paf.Ok():
		return nil
	case err = <-paf.Err():
		return err
	}
}

// AckWaitSec - max time in seconds to wait for an ack from memphis.
func AckWaitSec(ackWaitSec int) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.AckWaitSec = ackWaitSec
		return nil
	}
}
