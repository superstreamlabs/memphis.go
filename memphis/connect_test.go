package memphis

import (
	"fmt"
	"testing"
)

func TestConnect(t *testing.T) {
	got, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error()
	}

	fmt.Println(got)
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
