package memphis

import (
	"fmt"
	"testing"
)

func TestCreateFactory(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	_, err = c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}
}

func TestRemoveFactory(t *testing.T) {
	c, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Connection:", c)

	f, err := c.CreateFactory("factory_name_1", "factory_description")
	if err != nil {
		t.Error(err)
	}

	err = f.Remove()
	if err != nil {
		t.Error(err)
	}
}
