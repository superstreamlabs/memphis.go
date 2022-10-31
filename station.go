// Copyright 2021-2022 The Memphis Authors
// Licensed under the MIT License (the "License");
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// This license limiting reselling the software itself "AS IS".
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package memphis

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Station - memphis station object.
type Station struct {
	Name           string
	RetentionType  RetentionType
	RetentionValue int
	StorageType    StorageType
	Replicas       int
	DedupEnabled   bool
	DedupWindow    time.Duration
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
	RetentionType     string `json:"retention_type"`
	RetentionValue    int    `json:"retention_value"`
	StorageType       string `json:"storage_type"`
	Replicas          int    `json:"replicas"`
	DedupEnabled      bool   `json:"dedup_enabled"`
	DedupWindowMillis int    `json:"dedup_window_in_ms"`
}

type removeStationReq struct {
	Name string `json:"station_name"`
}

// StationsOpts - configuration options for a station.
type StationOpts struct {
	Name          string
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

func (c *Conn) CreateStation(Name string, opts ...StationOpt) (*Station, error) {
	defaultOpts := GetStationDefaultOptions()

	defaultOpts.Name = Name

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return nil, err
			}
		}
	}
	res, err := defaultOpts.createStation(c)
	if err != nil && strings.Contains(err.Error(), "already exist") {
		return res, nil
	}
	return res, err
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
		conn:           c,
	}

	return &s, s.conn.create(&s)

}

type StationName string

func (s *Station) Destroy() error {
	return s.conn.destroy(s)
}

func (s *Station) getCreationSubject() string {
	return "$memphis_station_creations"
}

func (s *Station) getCreationReq() any {
	return createStationReq{
		Name:              s.Name,
		RetentionType:     s.RetentionType.String(),
		RetentionValue:    s.RetentionValue,
		StorageType:       s.StorageType.String(),
		Replicas:          s.Replicas,
		DedupEnabled:      s.DedupEnabled,
		DedupWindowMillis: int(s.DedupWindow.Milliseconds()),
	}
}

func (s *Station) handleCreationResp(resp []byte) error {
	return defaultHandleCreationResp(resp)
}

func (s *Station) getDestructionSubject() string {
	return "$memphis_station_destructions"
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
		opts.Replicas = replicas
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

// Station schema updates related

type stationUpdateSub struct {
	refCount        int
	schemaUpdateCh  chan SchemaUpdate
	schemaUpdateSub *nats.Subscription
	schemaDetails   schemaDetails
}

type schemaDetails struct {
	name                string
	schemaVersions      map[int]string
	activeSchemaVersion int
	schemaType          string
}

func (c *Conn) listenToSchemaUpdates(stationName string) error {
	sus, ok := c.stationUpdatesSubs[stationName]
	if !ok {
		c.stationUpdatesSubs[stationName] = &stationUpdateSub{
			refCount:       1,
			schemaUpdateCh: make(chan SchemaUpdate),
			schemaDetails:  schemaDetails{},
		}
		sus := c.stationUpdatesSubs[stationName]
		schemaUpdatesSubject := fmt.Sprintf(schemaUpdatesSubjectTemplate, getInternalName(stationName))
		go sus.schemaUpdatesHandler(&c.stationUpdatesMu)
		var err error
		sus.schemaUpdateSub, err = c.brokerConn.Subscribe(schemaUpdatesSubject, sus.createMsgHandler())
		if err != nil {
			close(sus.schemaUpdateCh)
			return err
		}

		return nil
	}
	sus.refCount++
	return nil
}

func (sus *stationUpdateSub) createMsgHandler() nats.MsgHandler {
	return func(msg *nats.Msg) {
		var update SchemaUpdate
		err := json.Unmarshal(msg.Data, &update)
		if err != nil {
			log.Printf("schema update unmarshal error: %v\n", err)
			return
		}
		sus.schemaUpdateCh <- update
	}
}

func (c *Conn) removeSchemaUpdatesListener(stationName string) error {
	c.stationUpdatesMu.Lock()
	defer c.stationUpdatesMu.Unlock()
	sus, ok := c.stationUpdatesSubs[stationName]
	if !ok {
		return errors.New("listener doesn't exist")
	}

	sus.refCount--
	if sus.refCount <= 0 {
		close(sus.schemaUpdateCh)
		if err := sus.schemaUpdateSub.Unsubscribe(); err != nil {
			return err
		}
		delete(c.stationUpdatesSubs, stationName)
	}

	return nil
}

func (c *Conn) getSchemaDetails(stationName string) (schemaDetails, error) {
	c.stationUpdatesMu.RLock()
	defer c.stationUpdatesMu.RUnlock()

	sus, ok := c.stationUpdatesSubs[stationName]
	if !ok {
		return schemaDetails{}, errors.New("station subscription doesn't exist")
	}

	return sus.schemaDetails, nil
}

func (sus *stationUpdateSub) schemaUpdatesHandler(lock *sync.RWMutex) {
	for {
		update, ok := <-sus.schemaUpdateCh
		if !ok {
			return
		}

		lock.Lock()
		sd := &sus.schemaDetails
		switch update.UpdateType {
		case SchemaUpdateTypeInit:
			sd.handleSchemaUpdateInit(update.Init)
		case SchemaUpdateTypeDrop:
			sd.handleSchemaUpdateDrop()
		}
		lock.Unlock()
	}
}

func (sd *schemaDetails) handleSchemaUpdateInit(sui SchemaUpdateInit) {
	sd.name = sui.SchemaName
	sd.schemaVersions = make(map[int]string)
	for i, version := range sui.Versions {
		sd.schemaVersions[version.VersionNumber] = version.Descriptor
		if i == sui.ActiveVersionIdx {
			sd.activeSchemaVersion = version.VersionNumber
		}
	}
	sd.schemaType = sui.SchemaType
}

func (sd *schemaDetails) handleSchemaUpdateDrop() {
	*sd = schemaDetails{}
}
