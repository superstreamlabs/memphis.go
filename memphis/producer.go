package memphis

import (
	"errors"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
)

type Producer struct {
	Name    string
	station *Station
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

func (s *Station) CreateProducer(name string) (*Producer, error) {
	err := validateProducerName(name)
	if err != nil {
		return nil, err
	}

	p := Producer{Name: name, station: s}
	return &p, s.getConn().create(&p)
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
		StationName:  p.station.Name,
		ConnectionId: p.station.getConn().ConnId,
		ProducerType: "application",
	}
}

func (p *Producer) getDestructionApiPath() string {
	return "/api/producers/destroyProducer"
}

func (p *Producer) getDestructionReq() any {
	return RemoveProducerReq{Name: p.Name, StationName: p.station.Name}
}

func (p *Producer) Remove() error {
	return p.station.getConn().destroy(p)
}

func (p *Producer) Produce(message []byte, ackWaitSec int) (nats.PubAckFuture, error) {
	natsMessage := nats.Msg{
		Header: map[string][]string{"connectionId": {p.station.getConn().ConnId}, "producedBy": {p.Name}},
		Data:   message,
	}
	return p.station.publish(&natsMessage, nats.StallWait(time.Second*time.Duration(ackWaitSec)))
}
