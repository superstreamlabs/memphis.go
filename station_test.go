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
	"testing"
	"time"
)

func TestCreateStation(t *testing.T) {
	c, err := Connect("localhost", "root", ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s1, err := c.CreateStation("station_name_1", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), IdempotencyWindow(1*time.Second))
	if err != nil {
		t.Error(err)
	}

	s2, err := c.CreateStation("station_name_2", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), IdempotencyWindow(1*time.Second))
	if err != nil {
		t.Error(err)
	}

	s3, err := c.CreateStation("station_name_3")
	if err != nil {
		t.Error(err)
	}

	s1.Destroy()
	s2.Destroy()
	s3.Destroy()
}

func TestRemoveStation(t *testing.T) {
	c, err := Connect("localhost", "root", ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}

	err = s.Destroy()
	if err != nil {
		t.Error(err)
	}
}

func TestCreateStationWithDefaults(t *testing.T) {
	c, err := Connect("localhost", "root", ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s, err := c.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
	s.Destroy()
}
