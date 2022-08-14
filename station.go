// Copyright 2021-2022 The Memphis Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memphis

import "time"

// Station - memphis station object.
type Station struct {
	Name           string
	RetentionType  RetentionType
	RetentionValue int
	StorageType    StorageType
	Replicas       int
	DedupEnabled   bool
	DedupWindow    time.Duration
	factoryName    string
	conn           *Conn
}

// RetentionType - station's message retention type
type RetentionType int

const (
	MaxMessageAgeSeconds RetentionType = iota
	Messages
	Bytes
)

func (r RetentionType) String() string {
	return [...]string{"message_age_sec", "messages", "bytes"}[r]
}

// StorageType - station's message storage type
type StorageType int

const (
	File StorageType = iota
	Memory
)

func (s StorageType) String() string {
	return [...]string{"file", "memory"}[s]
}

type createStationReq struct {
	Name              string `json:"name"`
	FactoryName       string `json:"factory_name"`
	RetentionType     string `json:"retention_type"`
	RetentionValue    int    `json:"retention_value"`
	StorageType       string `json:"storage_type"`
	Replicas          int    `json:"replicas"`
	DedupEnabled      bool   `json:"dedup_enabled"`
	DedupWindowMillis int    `json:"dedup_window_in_ms"`
	Username    	  string `json:"username"`
}

type removeStationReq struct {
	Name string `json:"station_name"`
}

// StationsOpts - configuration options for a station.
type StationOpts struct {
	Name          string
	FactoryName   string
	RetentionType RetentionType
	RetentionVal  int
	StorageType   StorageType
	Replicas      int
	DedupEnabled  bool
	DedupWindow   time.Duration
}

// StationOpt - a function on the options for a station.
type StationOpt func(*StationOpts) error

// GetStationDefaultOptions - returns default configuration options for the station.
func GetStationDefaultOptions() StationOpts {
	return StationOpts{
		RetentionType: MaxMessageAgeSeconds,
		RetentionVal:  604800,
		StorageType:   File,
		Replicas:      1,
		DedupEnabled:  false,
		DedupWindow:   0 * time.Millisecond,
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
	res, err := defaultOpts.createStation(c)
	return res,err
}

func (opts *StationOpts) createStation(c *Conn) (*Station, error) {
	s := Station{
		Name:           opts.Name,
		RetentionType:  opts.RetentionType,
		RetentionValue: opts.RetentionVal,
		StorageType:    opts.StorageType,
		Replicas:       opts.Replicas,
		DedupEnabled:   opts.DedupEnabled,
		DedupWindow:    opts.DedupWindow,
		factoryName:    opts.FactoryName,
		conn:           c,
	}

	return &s, s.conn.createSubjectAndPublish(&s)

}

func (f *Factory) CreateStation(name string, opts ...StationOpt) (*Station, error) {
	return f.conn.CreateStation(name, f.Name, opts...)
}

type StationName string

func (s *Station) Destroy() error {
	return s.conn.destroy(s)
}

func (s *Station) getCreationApiPath() string {
	return "/api/stations/createStation"
}

func (s *Station) getCreationSubject() string {
	return "$memphis_station_creations"
}

func (s *Station) getCreationReq() any {
	return createStationReq{
		Name:              s.Name,
		FactoryName:       s.factoryName,
		RetentionType:     s.RetentionType.String(),
		RetentionValue:    s.RetentionValue,
		StorageType:       s.StorageType.String(),
		Replicas:          s.Replicas,
		DedupEnabled:      s.DedupEnabled,
		DedupWindowMillis: int(s.DedupWindow.Milliseconds()),
		Username:    	   s.conn.username,
	}
}

func (s *Station) getDestructionApiPath() string {
	return "/api/stations/removeStation"
}

func (s *Station) getDestructionReq() any {
	return removeStationReq{Name: s.Name}
}

// Name - station's name
func Name(name string) StationOpt {
	return func(opts *StationOpts) error {
		opts.Name = name
		return nil
	}
}

// FactoryName - factory name to link the station with.
func FactoryName(factoryName string) StationOpt {
	return func(opts *StationOpts) error {
		opts.FactoryName = factoryName
		return nil
	}
}

// RetentionTypeOpt - retention type, default is MaxMessageAgeSeconds.
func RetentionTypeOpt(retentionType RetentionType) StationOpt {
	return func(opts *StationOpts) error {
		opts.RetentionType = retentionType
		return nil
	}
}

// RetentionVal -  number which represents the retention based on the retentionType, default is 604800.
func RetentionVal(retentionVal int) StationOpt {
	return func(opts *StationOpts) error {
		opts.RetentionVal = retentionVal
		return nil
	}
}

// StorageTypeOpt - persistance storage for messages of the station, default is storageTypes.FILE.
func StorageTypeOpt(storageType StorageType) StationOpt {
	return func(opts *StationOpts) error {
		opts.StorageType = storageType
		return nil
	}
}

// Replicas - number of replicas for the messages of the data, default is 1.
func Replicas(replicas int) StationOpt {
	return func(opts *StationOpts) error {
		return nil
	}
}

// EnableDedup - whether to allow dedup mecanism, dedup happens based on message ID, default is false.
func EnableDedup() StationOpt {
	return func(opts *StationOpts) error {
		opts.DedupEnabled = true
		return nil
	}
}

// DedupWindow - time frame in which dedup track messages, default is 0.
func DedupWindow(dedupWindow time.Duration) StationOpt {
	return func(opts *StationOpts) error {
		opts.DedupWindow = dedupWindow
		return nil
	}
}
