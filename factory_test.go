package memphis

import (
	"testing"
)

func TestRemoveFactory(t *testing.T) {
	c, err := Connect("localhost", "root", "memphis")
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	f, err := c.CreateFactory("factory_name_1", Description("factory_description"))
	if err != nil {
		t.Error(err)
	}

	err = f.Destroy()
	if err != nil {
		t.Error(err)
	}
}
