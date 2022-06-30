package memphis

import (
	"fmt"
	"testing"
)

func TestCreateStation(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
	defer f.Remove()

	_, err = f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	_, err = f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err == nil {
		t.Error(err)
	}

	_, err = c.CreateStation("station_name_1", "factory_name_1", Messages, 0, Memory, 1, true, 1000)
	if err == nil {
		t.Error(err)
	}

	_, err = c.CreateStation("station_name_2", "factory_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	// station name is a globally unique identifier so next creation should fail
	_, err = c.CreateStation("station_name_1", "factory_name_2", Messages, 0, Memory, 1, true, 1000)
	if err == nil {
		t.Error(err)
	}
}

func TestRemoveStation(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
	s, err := f.CreateStation("station_name_1", Messages, 0, Memory, 1, true, 1000)
	if err != nil {
		t.Error(err)
	}

	err = s.Remove()
	if err != nil {
		t.Error(err)
	}
}
