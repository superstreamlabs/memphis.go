// Credit for The NATS.IO Authors
// Copyright 2021-2022 The Memphis Authors
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.package server

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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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
	name          string
	schemaType    string
	activeVersion SchemaVersion
	msgDescriptor protoreflect.MessageDescriptor
}

func (c *Conn) listenToSchemaUpdates(stationName string) error {
	sn := getInternalName(stationName)
	sus, ok := c.stationUpdatesSubs[sn]
	if !ok {
		c.stationUpdatesSubs[sn] = &stationUpdateSub{
			refCount:       1,
			schemaUpdateCh: make(chan SchemaUpdate),
			schemaDetails:  schemaDetails{},
		}
		sus := c.stationUpdatesSubs[sn]
		schemaUpdatesSubject := fmt.Sprintf(schemaUpdatesSubjectTemplate, sn)
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
	sn := getInternalName(stationName)

	c.stationUpdatesMu.Lock()
	defer c.stationUpdatesMu.Unlock()

	sus, ok := c.stationUpdatesSubs[sn]
	if !ok {
		return errors.New("listener doesn't exist")
	}

	sus.refCount--
	if sus.refCount <= 0 {
		close(sus.schemaUpdateCh)
		if err := sus.schemaUpdateSub.Unsubscribe(); err != nil {
			return err
		}
		delete(c.stationUpdatesSubs, sn)
	}

	return nil
}

func (c *Conn) getSchemaDetails(stationName string) (schemaDetails, error) {
	sn := getInternalName(stationName)

	c.stationUpdatesMu.RLock()
	defer c.stationUpdatesMu.RUnlock()

	sus, ok := c.stationUpdatesSubs[sn]
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
	sd.schemaType = sui.SchemaType
	sd.activeVersion = sui.ActiveVersion
	if sd.schemaType == "protobuf" {
		if err := sd.parseDescriptor(); err != nil {
			log.Println(err.Error())
		}
	}
}

func (sd *schemaDetails) handleSchemaUpdateDrop() {
	*sd = schemaDetails{}
}

func (sd *schemaDetails) parseDescriptor() error {
	descriptorSet := descriptorpb.FileDescriptorSet{}
	err := proto.Unmarshal([]byte(sd.activeVersion.Descriptor), &descriptorSet)
	if err != nil {
		return err
	}

	localRegistry, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		return err
	}

	filePath := fmt.Sprintf("%v_%v.proto", sd.name, sd.activeVersion.VersionNumber)
	fileDesc, err := localRegistry.FindFileByPath(filePath)
	if err != nil {
		return err
	}

	msgsDesc := fileDesc.Messages()
	msgDesc := msgsDesc.ByName(protoreflect.Name(sd.activeVersion.MessageStructName))

	sd.msgDescriptor = msgDesc
	return nil
}

func (sd *schemaDetails) validateMsg(msg any) ([]byte, error) {
	switch sd.schemaType {
	case "protobuf":
		return sd.validateProtoMsg(msg)
	default:
		return nil, errors.New("Invalid schema type")
	}
}

func (sd *schemaDetails) validateProtoMsg(msg any) ([]byte, error) {
	var (
		msgBytes []byte
		err      error
	)
	switch msg.(type) {
	case protoreflect.ProtoMessage:
		msgBytes, err = proto.Marshal(msg.(protoreflect.ProtoMessage))
		if err != nil {
			return nil, err
		}
	case []byte:
		msgBytes = msg.([]byte)
	default:
		return nil, errors.New("Unsupported message type")
	}

	protoMsg := dynamicpb.NewMessage(sd.msgDescriptor)
	err = proto.Unmarshal(msgBytes, protoMsg)
	if err != nil {
		return nil, err
	}

	return msgBytes, nil
}
