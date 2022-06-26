package memphis

import (
	"time"

	"github.com/nats-io/nats.go"
)

type CreateProducerReq struct {
	Name         string `json:"name"`
	StationName  string `json:"station_name"`
	ConnectionId string `json:"connection_id"`
	ProducerType string `json:"producer_type"`
}

func (s *Station) CreateProducer(name string) (Producer, error) {
	return Producer{Name: name, station: s},
		s.conn.managementRequest("POST", "/api/producers/createProducer", CreateProducerReq{
			Name: name, StationName: s.Name, ConnectionId: s.conn.ConnId, ProducerType: "application"})
}

type Producer struct {
	Name    string
	station *Station
}

func (p *Producer) Produce(message []byte, ackWaitSec int) {
	natsMessage := nats.Msg{
		Subject: p.station.getSubjectName(),
		Header:  map[string][]string{"producedBy": {p.Name}},
		Data:    message,
	}
	p.station.conn.brokerConn.PublishMsg(&natsMessage, nats.StallWait(time.Second*time.Duration(ackWaitSec)))
}
