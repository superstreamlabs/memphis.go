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
	"time"

	"github.com/nats-io/nats.go"
)

const (
	schemaUpdatesSubjectTemplate = "$memphis_schema_updates_%s"
)

// Producer - memphis producer object.
type Producer struct {
	Name            string
	stationName     string
	conn            *Conn
	schemaUpdateCh  chan *nats.Msg
	schemaUpdateSub *nats.Subscription
	schemaDetails   schemaDetails
}

type schemaDetails struct {
	name                string
	schemaVersions      map[int]string
	activeSchemaVersion int
	schemaType          string
}

type createProducerReq struct {
	Name         string `json:"name"`
	StationName  string `json:"station_name"`
	ConnectionId string `json:"connection_id"`
	ProducerType string `json:"producer_type"`
}

type createProducerResp struct {
	SchemaUpdateInit SchemaUpdateInit `json:"schema_update"`
	Err              string           `json:"error"`
}

type SchemaUpdateType int

const (
	SchemaUpdateTypeInit SchemaUpdateType = iota + 1
	SchemaUpdateTypeNewVersion
	SchemaUpdateTypeChangeVersion
	SchemaUpdateTypeDrop
)

type SchemaUpdate struct {
	UpdateType    SchemaUpdateType
	Init          SchemaUpdateInit          `json:"init,omitempty"`
	NewVersion    SchemaUpdateNewVersion    `json:"new_version,omitempty"`
	ChangeVersion SchemaUpdateChangeVersion `json:"change_version,omitempty"`
}

type SchemaUpdateInit struct {
	SchemaName       string          `json:"schema_name"`
	Versions         []SchemaVersion `json:"versions"`
	ActiveVersionIdx int             `json:"active_index"`
	SchemaType       string          `json:"type"`
}

type SchemaUpdateNewVersion struct {
	Version SchemaVersion `json:"version_details"`
}

type SchemaUpdateChangeVersion struct {
	VersionNumber int `json:"version_number"`
}

type SchemaVersion struct {
	VersionNumber int    `json:"version_number"`
	Descriptor    string `json:"schema_content"`
}

type removeProducerReq struct {
	Name        string `json:"name"`
	StationName string `json:"station_name"`
}

// ProducerOpts - configuration options for producer creation.
type ProducerOpts struct {
	GenUniqueSuffix bool
}

// ProducerOpt - a function on the options for producer creation.
type ProducerOpt func(*ProducerOpts) error

// getDefaultProducerOpts - returns default configuration options for producer creation.
func getDefaultProducerOpts() ProducerOpts {
	return ProducerOpts{GenUniqueSuffix: false}
}

func extendNameWithRandSuffix(name string) (string, error) {
	suffix, err := randomHex(4)
	if err != nil {
		return "", err
	}
	return name + "_" + suffix, err
}

// CreateProducer - creates a producer.
func (c *Conn) CreateProducer(stationName, name string, opts ...ProducerOpt) (*Producer, error) {
	defaultOpts := getDefaultProducerOpts()
	var err error
	for _, opt := range opts {
		if err = opt(&defaultOpts); err != nil {
			return nil, err
		}
	}

	if defaultOpts.GenUniqueSuffix {
		name, err = extendNameWithRandSuffix(name)
		if err != nil {
			return nil, err
		}
	}

	schemaUpdatesSubject := fmt.Sprintf(schemaUpdatesSubjectTemplate, getInternalName(stationName))

	p := Producer{
		Name:           name,
		stationName:    stationName,
		conn:           c,
		schemaUpdateCh: make(chan *nats.Msg),
	}

	go p.schemaUpdatesHandler()
	p.schemaUpdateSub, err = c.brokerConn.ChanSubscribe(schemaUpdatesSubject, p.schemaUpdateCh)
	if err != nil {
		close(p.schemaUpdateCh)
		return nil, err
	}

	if err = c.create(&p); err != nil {
		if err = p.schemaUpdateSub.Unsubscribe(); err != nil {
			log.Printf("unsubscribe failed: %v\n", err)
		}
		close(p.schemaUpdateCh)
		return nil, err
	}

	return &p, nil
}

func (p *Producer) schemaUpdatesHandler() {
	for {
		select {
		case msg, ok := <-p.schemaUpdateCh:
			if !ok {
				return
			}
			var update SchemaUpdate
			err := json.Unmarshal(msg.Data, &update)
			if err != nil {
				log.Printf("schema update unmarshal error: %v\n", err)
				continue
			}

		}
	}
}

func (p *Producer) handleSchemaUpdate(su SchemaUpdate) {
	sd := &p.schemaDetails
	switch su.UpdateType {
	case SchemaUpdateTypeInit:
		sd.handleSchemaUpdateInit(su.Init)
	case SchemaUpdateTypeNewVersion:
		sd.handleSchemaUpdateNewVersion(su.NewVersion)
	case SchemaUpdateTypeChangeVersion:
		sd.handleSchemaUpdateChangeVersion(su.ChangeVersion)
	case SchemaUpdateTypeDrop:
		sd.handleSchemaUpdatfeDrop()
	}
}

// Station.CreateProducer - creates a producer attached to this station.
func (s *Station) CreateProducer(name string, opts ...ProducerOpt) (*Producer, error) {
	return s.conn.CreateProducer(s.Name, name, opts...)
}

func (p *Producer) getCreationSubject() string {
	return "$memphis_producer_creations"
}

func (p *Producer) getCreationReq() any {
	return createProducerReq{
		Name:         p.Name,
		StationName:  p.stationName,
		ConnectionId: p.conn.ConnId,
		ProducerType: "application",
	}
}

func (p *Producer) handleCreationResp(resp []byte) error {
	cr := &createProducerResp{}
	err := json.Unmarshal(resp, cr)
	if err != nil {
		return err
	}

	if cr.Err != "" {
		return errors.New(cr.Err)
	}

	p.schemaDetails.handleSchemaUpdateInit(cr.SchemaUpdateInit)
	return nil
}

func (sd *schemaDetails) handleSchemaUpdateInit(sui SchemaUpdateInit) {
	sd.name = sui.SchemaName
	for i, version := range sui.Versions {
		sd.schemaVersions[version.VersionNumber] = version.Descriptor
		if i == sui.ActiveVersionIdx {
			sd.activeSchemaVersion = version.VersionNumber
		}
	}
	sd.schemaType = sui.SchemaType
}

func (sd *schemaDetails) handleSchemaUpdateNewVersion(sunv SchemaUpdateNewVersion) {
	nv := sunv.Version
	_, ok := sd.schemaVersions[nv.VersionNumber]
	if ok {
		if sd.schemaVersions[nv.VersionNumber] != nv.Descriptor {
			panic("received different descriptor for existing version number")
		}
		return
	}

	sd.schemaVersions[nv.VersionNumber] = nv.Descriptor
}

func (sd *schemaDetails) handleSchemaUpdateChangeVersion(sucv SchemaUpdateChangeVersion) {
	_, ok := sd.schemaVersions[sucv.VersionNumber]
	if !ok {
		panic("activating non-existing version number")
	}
	sd.activeSchemaVersion = sucv.VersionNumber
}

func (sd *schemaDetails) handleSchemaUpdatfeDrop() {
	sd = &schemaDetails{}
}

func (p *Producer) getDestructionSubject() string {
	return "$memphis_producer_destructions"
}

func (p *Producer) getDestructionReq() any {
	return removeProducerReq{Name: p.Name, StationName: p.stationName}
}

// Destroy - destoy this producer.
func (p *Producer) Destroy() error {
	return p.conn.destroy(p)
}

type Headers struct {
	MsgHeaders map[string][]string
}

// ProduceOpts - configuration options for produce operations.
type ProduceOpts struct {
	Message      []byte
	AckWaitSec   int
	MsgHeaders   Headers
	AsyncProduce bool
}

// ProduceOpt - a function on the options for produce operations.
type ProduceOpt func(*ProduceOpts) error

// getDefaultProduceOpts - returns default configuration options for produce operations.
func getDefaultProduceOpts() ProduceOpts {
	return ProduceOpts{AckWaitSec: 15, MsgHeaders: Headers{}, AsyncProduce: false}

}

// Producer.Produce - produces a message into a station.
func (p *Producer) Produce(message []byte, opts ...ProduceOpt) error {
	defaultOpts := getDefaultProduceOpts()
	defaultOpts.Message = message

	for _, opt := range opts {
		if opt != nil {
			if err := opt(&defaultOpts); err != nil {
				return err
			}
		}
	}

	return defaultOpts.produce(p)
}

func (hdr *Headers) validateHeaderKey(key string) error {
	if strings.HasPrefix(key, "$memphis") {
		return errors.New("Keys in headers should not start with $memphis")
	}
	return nil
}

func (hdr *Headers) New() {
	hdr.MsgHeaders = map[string][]string{}
}

func (hdr *Headers) Add(key, value string) error {
	err := hdr.validateHeaderKey(key)
	if err != nil {
		return err
	}

	hdr.MsgHeaders[key] = []string{value}
	return nil
}

// ProducerOpts.produce - produces a message into a station using a configuration struct.
func (opts *ProduceOpts) produce(p *Producer) error {
	opts.MsgHeaders.MsgHeaders["$memphis_connectionId"] = []string{p.conn.ConnId}
	opts.MsgHeaders.MsgHeaders["$memphis_producedBy"] = []string{p.Name}

	natsMessage := nats.Msg{
		Header:  opts.MsgHeaders.MsgHeaders,
		Subject: getInternalName(p.stationName) + ".final",
		Data:    opts.Message,
	}

	stallWaitDuration := time.Second * time.Duration(opts.AckWaitSec)
	paf, err := p.conn.brokerPublish(&natsMessage, nats.StallWait(stallWaitDuration))
	if err != nil {
		return err
	}

	if opts.AsyncProduce {
		return nil
	}

	select {
	case <-paf.Ok():
		return nil
	case err = <-paf.Err():
		return err
	}
}

// ProducerGenUniqueSuffix - whether to generate a unique suffix for this producer.
func ProducerGenUniqueSuffix() ProducerOpt {
	return func(opts *ProducerOpts) error {
		opts.GenUniqueSuffix = true
		return nil
	}
}

// AckWaitSec - max time in seconds to wait for an ack from memphis.
func AckWaitSec(ackWaitSec int) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.AckWaitSec = ackWaitSec
		return nil
	}
}

// MsgHeaders - set headers to a message
func MsgHeaders(hdrs Headers) ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.MsgHeaders = hdrs
		return nil
	}
}

// AsyncProduce - produce operation won't wait for broker acknowledgement
func AsyncProduce() ProduceOpt {
	return func(opts *ProduceOpts) error {
		opts.AsyncProduce = true
		return nil
	}
}
