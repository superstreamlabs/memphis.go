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

	err = c.Produce("station_name_c_produce", "producer_name_a", []byte("Hey There!"))
	if err != nil {
		t.Error(err)
	}

	err = c.Produce("station_name_c_produce", "producer_name_a", []byte("Hey! Test 2 pleaseee"))
	if err != nil {
		t.Error(err)
	}
}
