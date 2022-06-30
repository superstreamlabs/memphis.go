package memphis

import (
	"errors"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
)

type Producer struct {
	Name        string
	stationName string
	conn        *Conn
}

type CreateProducerReq struct {
	Name         string `json:"name"`
	StationName  string `json:"station_name"`
	ConnectionId string `json:"connection_id"`
	ProducerType string `json:"producer_type"`
}

type RemoveProducerReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
}

func (c *Conn) CreateProducer(name string, stationName string) (*Producer, error) {
	err := validateProducerName(name)
	if err != nil {
		return nil, err
	}

	p := Producer{Name: name, stationName: stationName, conn: c}
	return &p, c.create(&p)
}

func (s *Station) CreateProducer(name string) (*Producer, error) {
	return s.getConn().CreateProducer(name, s.Name)
}

func validateProducerName(name string) error {
	regex := regexp.MustCompile("^[a-z_]+$")
	if !regex.MatchString(name) {
		return errors.New("Producer name can only contain lower-case letters and _")
	}
	return nil
}

func (p *Producer) getCreationApiPath() string {
	return "/api/producers/createProducer"
}

func (p *Producer) getCreationReq() any {
	return CreateProducerReq{
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
	return RemoveProducerReq{Name: p.Name, StationName: p.stationName}
}

func (p *Producer) Remove() error {
	return p.conn.destroy(p)
}

func (p *Producer) Produce(message []byte, ackWaitSec int) (nats.PubAckFuture, error) {
	natsMessage := nats.Msg{
		Header:  map[string][]string{"connectionId": {p.conn.ConnId}, "producedBy": {p.Name}},
		Subject: getSubjectName(p.stationName),
		Data:    message,
	}
	return p.conn.brokerPublish(&natsMessage, nats.StallWait(time.Second*time.Duration(ackWaitSec)))
}
