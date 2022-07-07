package memphis

import (
	"time"

	"github.com/nats-io/nats.go"
)

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

func (c *Conn) CreateProducer(stationName, name string) (*Producer, error) {
	p := Producer{Name: name, stationName: stationName, conn: c}
	return &p, c.create(&p)
}

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

func (p *Producer) Destroy() error {
	return p.conn.destroy(p)
}

type ProduceOpts struct {
	Message    []byte
	AckWaitSec int
}

type ProduceOpt func(*ProduceOpts) error

func GetDefaultProduceOpts() ProduceOpts {
	return ProduceOpts{AckWaitSec: 15}
}

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

func AckWaitSec(ackWaitSec int) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.AckWaitSec = ackWaitSec
		return nil
	}
}
