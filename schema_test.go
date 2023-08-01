package memphis

import (
	"fmt"
	"testing"
)

func TestCreateSchema(t *testing.T) {
	c, err := Connect("localhost", "root", Password("memphis"))
	if err != nil {
		fmt.Println(err.Error())
	}
	defer c.Close()

	err = c.CreateSchema("sdk_test_schema_graphql", "graphql", "./test_schemas/test.graphqls")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("qraphql Created!!")
	}

	err = c.CreateSchema("sdk_test_schema_protobuf2", "protobuf", "./test_schemas/test_p2.proto")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("protobuf Created!!")
	}

	err = c.CreateSchema("sdk_test_schema_protobuf3", "protobuf", "./test_schemas/test_p3.proto")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("protobuf Created!!")
	}

	err = c.CreateSchema("sdk_test_schema_json", "json", "./test_schemas/test.json")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("json Created!!")
	}

	err = c.CreateSchema("sdk_test_schema_avro", "avro", "./test_schemas/test.avsc")
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("avro Created!!")
	}
}
