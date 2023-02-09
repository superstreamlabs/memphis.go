package memphis

import (
	"testing"
)

func TestConnect(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error()
		return
	}
	c.Close()
}

func TestNormalizeHost(t *testing.T) {
	if "www.google.com" != normalizeHost("http://www.google.com") {
		t.Error()
	}

	if "www.yahoo.com" != normalizeHost("https://www.yahoo.com") {
		t.Error()
	}

	if "http.http.http://" != normalizeHost("http://http.http.http://") {
		t.Error()
	}
}

func TestProduceNoProducer(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	err = c.Produce("station_name_c_produce", "producer_name_a", []byte("Hey There!"), nil, nil)
	if err != nil {
		t.Error(err)
	}

	err = c.Produce("station_name_c_produce", "producer_name_a", []byte("Hey! Test 2 pleaseee"), nil, nil)
	if err != nil {
		t.Error(err)
	}

	pm := c.getProducersMap()
	pm.unsetStationProducers("station_name_c_produce")
	p := pm.getProducer("producer_name_a")
	if p != nil {
		t.Error("unsetStationProducers failed to remove key [station_name_c_produce]")
	}
}
