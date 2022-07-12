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

	f, err := c.CreateFactory("factory_name_1")
	if err != nil {
		t.Error(err)
	}
	defer f.Destroy()

	_, err = f.CreateStation("station_name_1", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), EnableDedup(), DedupWindow(1*time.Second))
	if err != nil {
		t.Error(err)
	}

	_, err = f.CreateStation("station_name_1", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), EnableDedup(), DedupWindow(1*time.Second))
	if err == nil {
		t.Error(err)
	}

	_, err = c.CreateStation("station_name_1", "factory_name_1", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), EnableDedup(), DedupWindow(1*time.Second))
	if err == nil {
		t.Error(err)
	}

	_, err = c.CreateStation("station_name_2", "factory_name_1", RetentionTypeOpt(Messages), RetentionVal(0), StorageTypeOpt(Memory), Replicas(1), EnableDedup(), DedupWindow(1*time.Second))
	if err != nil {
		t.Error(err)
	}

	// station name is a globally unique identifier so next creation should fail
	_, err = c.CreateStation("station_name_1", "factory_name_2")
	if err == nil {
		t.Error(err)
	}

	// this creates another factory so we need to clean it
	_, err = c.CreateStation("station_name_3", "factory_name_2")
	if err != nil {
		t.Error(err)
	}
	defer c.destroy(&Factory{Name: "factory_name_2", Description: ""})
}

func TestRemoveStation(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	f, err := c.CreateFactory("factory_name_1")
	if err != nil {
		t.Error(err)
	}
	defer f.Destroy()

	s, err := f.CreateStation("station_name_1")
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

	f, err := c.CreateFactory("factory_name_1")
	if err != nil {
		t.Error(err)
	}
	defer f.Destroy()

	_, err = f.CreateStation("station_name_1")
	if err != nil {
		t.Error(err)
	}
}
