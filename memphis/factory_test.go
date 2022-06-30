package memphis

import (
	"testing"
)

func TestRemoveFactory(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}

	err = f.Remove()
	if err != nil {
		t.Error(err)
	}
}
