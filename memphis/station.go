package memphis

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

func (c *Conn) CreateStation(name string,
	factoryName string,
	retentionType RetentionType,
	retentionVal int,
	storageType StorageType,
	replicas int,
	dedupEnabled bool,
	dedupWindowMillis int) (Station, error) {
	return Station{Name: name, conn: c}, c.managementRequest("POST", "/api/stations/createStation", CreateStationReq{
		Name:              name,
		FactoryName:       factoryName,
		RetentionType:     retentionType.String(),
		RetentionValue:    retentionVal,
		StorageType:       storageType.String(),
		Replicas:          replicas,
		DedupEnabled:      dedupEnabled,
		DedupWindowMillis: dedupWindowMillis,
	})
}

func (c *Conn) RemoveStation(name string) error {
	return c.managementRequest("DELETE", "/api/stations/removeStation", RemoveStationReq{Name: name})
}

func (s *Station) Remove() error {
	return s.conn.RemoveStation(s.Name)
}

type Station struct {
	Name string
	conn *Conn
}

func (s *Station) getSubjectName() string {
	return s.Name + ".final"
}
