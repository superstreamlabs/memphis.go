package memphis

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/graph-gophers/graphql-go"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

const (
	schemaObjectName = "Schema"
)

type Schema struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	CreatedByUsername string `json:"created_by_username"`
	SchemaContent     string `json:"schema_content"`
	MessageStructName string `json:"message_struct_name"`
}

type createSchemaReq struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	CreatedByUsername string `json:"created_by_username"`
	SchemaContent     string `json:"schema_content"`
	MessageStructName string `json:"message_struct_name"`
}

type createSchemaResp struct {
	Err string `json:"error"`
}

type removeSchemaReq struct {
	Name string `json:"name"`
}

func (s *Schema) getCreationSubject() string {
	return "$memphis_schema_creations"
}

func (s *Schema) getDestructionSubject() string {
	return ""
}

func (s *Schema) getCreationReq() any {
	return createSchemaReq{
		Name:              s.Name,
		Type:              s.Type,
		CreatedByUsername: s.CreatedByUsername,
		SchemaContent:     s.SchemaContent,
	}
}

func (s *Schema) handleCreationResp(resp []byte) error {
	cr := &createSchemaResp{}
	err := json.Unmarshal(resp, cr)
	if err != nil {
		return defaultHandleCreationResp(resp)
	}

	if cr.Err != "" {
		return memphisError(errors.New(cr.Err))
	}
	return nil
}

func (s *Schema) getDestructionReq() any {
	return nil
}

func (c *Conn) CreateSchema(name, schemaType, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return memphisError(err)
	}

	schemaContent := string(data)

	err = validateSchemaContent(schemaContent, schemaType)
	if err != nil {
		return memphisError(err)
	}

	err = validateSchemaName(name)
	if err != nil {
		return memphisError(err)
	}

	err = validateSchemaType(schemaType)
	if err != nil {
		return memphisError(err)
	}

	//fix the message struct name with protobuf
	s := Schema{
		Name:              name,
		Type:              schemaType,
		CreatedByUsername: c.username,
		SchemaContent:     schemaContent,
		MessageStructName: "",
	}

	if err = c.create(&s); err != nil {
		return memphisError(err)
	}

	return nil
}

func validateSchemaContent(schemaContent, schemaType string) error {
	if len(schemaContent) == 0 {
		return errors.New("your schema content is invalid")
	}

	switch schemaType {
	case "protobuf":
		err := validateProtobufContent(schemaContent)
		if err != nil {
			return err
		}
	case "json":
		err := validateJsonSchemaContent(schemaContent)
		if err != nil {
			return err
		}
	case "graphql":
		err := validateGraphqlSchemaContent(schemaContent)
		if err != nil {
			return err
		}
	case "avro":
		break
	}
	return nil
}

func validateProtobufContent(schemaContent string) error {
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(schemaContent)), nil
		},
	}
	_, err := parser.ParseFiles("")
	if err != nil {
		return errors.New("your Proto file is invalid: " + err.Error())
	}

	return nil
}

func validateJsonSchemaContent(schemaContent string) error {
	_, err := jsonschema.CompileString("test", schemaContent)
	if err != nil {
		return errors.New("your json schema is invalid")
	}

	return nil
}

func validateGraphqlSchemaContent(schemaContent string) error {
	_, err := graphql.ParseSchema(schemaContent, nil)
	if err != nil {
		return err
	}
	return nil
}

func validateSchemaName(schemaName string) error {
	return validateName(schemaName, schemaObjectName)
}

func validateName(name, objectType string) error {
	emptyErrStr := fmt.Sprintf("%v name can not be empty", objectType)
	tooLongErrStr := fmt.Sprintf("%v should be under 128 characters", objectType)
	invalidCharErrStr := fmt.Sprintf("Only alphanumeric and the '_', '-', '.' characters are allowed in %v", objectType)
	firstLetterErrStr := fmt.Sprintf("%v name can not start or end with non alphanumeric character", objectType)

	emptyErr := errors.New(emptyErrStr)
	tooLongErr := errors.New(tooLongErrStr)
	invalidCharErr := errors.New(invalidCharErrStr)
	firstLetterErr := errors.New(firstLetterErrStr)

	if len(name) == 0 {
		return emptyErr
	}

	if len(name) > 128 {
		return tooLongErr
	}

	re := regexp.MustCompile("^[a-z0-9_.-]*$")

	validName := re.MatchString(name)
	if !validName {
		return invalidCharErr
	}

	if name[0:1] == "." || name[0:1] == "-" || name[0:1] == "_" || name[len(name)-1:] == "." || name[len(name)-1:] == "-" || name[len(name)-1:] == "_" {
		return firstLetterErr
	}

	return nil
}

func validateSchemaType(schemaType string) error {
	invalidTypeErrStr := "unsupported schema type"
	invalidTypeErr := errors.New(invalidTypeErrStr)
	invalidSupportTypeErrStr := "avro is not supported at this time"
	invalidSupportTypeErr := errors.New(invalidSupportTypeErrStr)

	if schemaType == "protobuf" || schemaType == "json" || schemaType == "graphql" {
		return nil
	} else if schemaType == "avro" {
		return invalidSupportTypeErr
	} else {
		return invalidTypeErr
	}
}
