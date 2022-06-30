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
	factoryName       string
	conn              *Conn
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

type GetStationReq struct {
	Name string `json:"station_name"`
}
type StationOpts struct {
	Name              string
	FactoryName       string
	RetentionType     RetentionType
	RetentionVal      int
	StorageType       StorageType
	Replicas          int
	DedupEnabled      bool
	DedupWindowMillis int
}

type StationOpt func(*StationOpts) error

func GetStationDefaultOptions() StationOpts {
	return StationOpts{
		RetentionType:     MaxMessageAgeSeconds,
		RetentionVal:      604800,
		StorageType:       File,
		Replicas:          1,
		DedupEnabled:      false,
		DedupWindowMillis: 0,
	}
}

func (c *Conn) CreateStation(Name, FactoryName string, opts ...StationOpt) (*Station, error) {
	defaultOpts := GetStationDefaultOptions()

	defaultOpts.Name = Name
	defaultOpts.FactoryName = FactoryName

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, err
			}
		}
	}

	return defaultOpts.CreateStation(c)
}

func (opts *StationOpts) CreateStation(c *Conn) (*Station, error) {
	s := Station{
		Name:              opts.Name,
		RetentionType:     opts.RetentionType,
		RetentionValue:    opts.RetentionVal,
		StorageType:       opts.StorageType,
		Replicas:          opts.Replicas,
		DedupEnabled:      opts.DedupEnabled,
		DedupWindowMillis: opts.DedupWindowMillis,
		factoryName:       opts.FactoryName,
		conn:              c,
	}

	return &s, s.getConn().create(&s)
}

func (f *Factory) CreateStation(name string, opts ...StationOpt) (*Station, error) {
	return f.conn.CreateStation(name, f.Name, opts...)
}

type StationName string

func (s *Station) Remove() error {
	return s.getConn().destroy(s)
}

func (s *Station) getSubjectName() string {
	return getSubjectName(s.Name)
}

func getSubjectName(stationName string) string {
	return stationName + ".final"
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
		FactoryName:       s.factoryName,
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
	return s.conn
}

func Name(name string) StationOpt {
	return func(opts *StationOpts) error {
		opts.Name = name
		return nil
	}
}

func FactoryName(factoryName string) StationOpt {
	return func(opts *StationOpts) error {
		opts.FactoryName = factoryName
		return nil
	}
}

func RetentionTypeOpt(retentionType RetentionType) StationOpt {
	return func(opts *StationOpts) error {
		opts.RetentionType = retentionType
		return nil
	}
}

func RetentionVal(retentionVal int) StationOpt {
	return func(opts *StationOpts) error {
		opts.RetentionVal = retentionVal
		return nil
	}
}

func StorageTypeOpt(storageType StorageType) StationOpt {
	return func(opts *StationOpts) error {
		opts.StorageType = storageType
		return nil
	}
}

func Replicas(replicas int) StationOpt {
	return func(opts *StationOpts) error {
		return nil
	}
}

func EnableDedup() StationOpt {
	return func(opts *StationOpts) error {
		opts.DedupEnabled = true
		return nil
	}
}

func DedupWindowMillis(dedupWindowMillis int) StationOpt {
	return func(opts *StationOpts) error {
		opts.DedupWindowMillis = dedupWindowMillis
		return nil
	}
}
