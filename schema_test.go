package memphis

import (
	"fmt"
	"testing"
)

func TestCreateSchema(t *testing.T) {
	c, err := Connect("localhost", "schematest", Password("1234"))
	if err != nil {
		fmt.Printf(err.Error())
	}
	defer c.Close()

	err = c.CreateSchema("sdk_test_schema", "json", "./s.json")
	if err != nil {
		fmt.Printf(err.Error())
	}
	fmt.Println("success!!")

}
