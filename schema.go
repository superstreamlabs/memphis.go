package memphis

import (
	"encoding/json"
	"errors"
)

/**  implement this interface

type directObj interface {
	getCreationSubject() string ****
	getCreationReq() any ****
	handleCreationResp([]byte) error ****
	getDestructionSubject() string ****
	getDestructionReq() any ****
}

**/

type Schema struct {
	ID                int    `json:"id"`
	Name              string `json:"name"`
	Type              string `json:"type"`
	CreatedByUsername string `json:"created_by_username"`
	TenantName        string `json:"tenant_name"`
	SchemaContent     string `json:"schema_content"`
}

type createSchemaReq struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	CreatedByUsername string `json:"created_by_username"`
	TenantName        string `json:"tenant_name"`
	SchemaContent     string `json:"schema_content"`
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
	return "$memphis_schema_destructions"
}

func (s *Schema) getCreationReq() any {
	return createSchemaReq{
		Name:              s.Name,
		Type:              s.Type,
		CreatedByUsername: s.CreatedByUsername,
		TenantName:        s.TenantName,
		schemaContent:     s.SchemaContent,
	}
}

// add more error cases and success cases
func (s *Schema) handleCreationResp(resp []byte) error {
	cr := &createSchemaResp{}
	err := json.Unmarshal(resp, cr)
	if err != nil {
		return defaultHandleCreationResp(resp)
	}

	if cr.Err != "" {
		return memphisError(errors.New(cr.Err))
	}
}

func (s *Schema) getDestructionReq() any {
	return removeSchemaReq{Name: s.Name}
}
