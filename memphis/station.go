package memphis

import "github.com/nats-io/nats.go"

type Station struct {
	Name              string
	RetentionType     RetentionType
	RetentionValue    int
	StorageType       StorageType
	Replicas          int
	DedupEnabled      bool
	DedupWindowMillis int
	factory           *Factory
}

type RetentionType int

const (
	MaxMessageAgeSeconds RetentionType = iota
	Messages
	Bytes
)

func (r RetentionType) String() string {
	return [...]string{"message_age_sec", "messages", "bytes"}[r]
}

type StorageType int

const (
	File StorageType = iota
	Memory
)

func (s StorageType) String() string {
	return [...]string{"file", "memory"}[s]
}

type CreateStationReq struct {
	Name              string `json:"name"`
	FactoryName       string `json:"factory_name"`
	RetentionType     string `json:"retention_type"`
	RetentionValue    int    `json:"retention_value"`
	StorageType       string `json:"storage_type"`
	Replicas          int    `json:"replicas"`
	DedupEnabled      bool   `json:"dedup_enabled"`
	DedupWindowMillis int    `json:"dedup_window_in_ms"`
}

type RemoveStationReq struct {
	Name string `json:"station_name"`
}

func (f *Factory) CreateStation(name string,
	retentionType RetentionType,
	retentionVal int,
	storageType StorageType,
	replicas int,
	dedupEnabled bool,
	dedupWindowMillis int) (*Station, error) {
	s := Station{
		Name:              name,
		RetentionType:     retentionType,
		RetentionValue:    retentionVal,
		StorageType:       storageType,
		Replicas:          replicas,
		DedupEnabled:      dedupEnabled,
		DedupWindowMillis: dedupWindowMillis,
		factory:           f,
	}

	return &s, s.getConn().create(&s)
}

func (s *Station) Remove() error {
	return s.getConn().destroy(s)
}

func (s *Station) getSubjectName() string {
	return s.Name + ".final"
}

func (s *Station) publish(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	msg.Subject = s.getSubjectName()
	return s.getConn().brokerPublish(msg, opts...)
}

func (s *Station) subscribe(c *Consumer, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return s.getConn().brokerSubscribe(s.getSubjectName(), c.ConsumerGroup, opts...)
}

func (s *Station) getCreationApiPath() string {
	return "/api/stations/createStation"
}

func (s *Station) getCreationReq() any {
	return CreateStationReq{
		Name:              s.Name,
		FactoryName:       s.factory.Name,
		RetentionType:     s.RetentionType.String(),
		RetentionValue:    s.RetentionValue,
		StorageType:       s.StorageType.String(),
		Replicas:          s.Replicas,
		DedupEnabled:      s.DedupEnabled,
		DedupWindowMillis: s.DedupWindowMillis,
	}
}

func (s *Station) getDestructionApiPath() string {
	return "/api/stations/removeStation"
}

func (s *Station) getDestructionReq() any {
	return RemoveStationReq{Name: s.Name}
}

func (s *Station) getConn() *Conn {
	return s.factory.getConn()
}
