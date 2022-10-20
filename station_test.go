package memphis

import (
	"testing"
	"time"
)

func TestCreateStation(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	s1, err := c.CreateStation("station_name_1", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), EnableDedup(), DedupWindow(1*time.Second))
	if err != nil {
		t.Error(err)
	}

	s2, err := c.CreateStation("station_name_2", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), EnableDedup(), DedupWindow(1*time.Second))
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
	c, err := Connect("localhost", "root", "memphis")
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
	c, err := Connect("localhost", "root", "memphis")
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
