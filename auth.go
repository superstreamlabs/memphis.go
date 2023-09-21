package memphis

import (
	"encoding/json"
	"time"
)

const (
	accessTokenValidationSubject = "$memphis_access_token_validation"
	accessTokenGenerationSubject = "$memphis_access_token_generation"
)

type AccessToken struct {
	AccessKeyID string `json:"access_key_id"`
	SecretKey   string `json:"secret_key"`
}

type generateAccessTokenReq struct {
	Description string `json:"description"`
	Username    string `json:"username"`
}

type generateAccessTokenResp struct {
	AccessKeyID string `json:"access_key_id"`
	SecretKey   string `json:"secret_key"`
	Err         string `json:"error"`
}

type validateAccessTokenReq struct {
	AccessKeyID string `json:"access_key_id"`
	SecretKey   string `json:"secret_key"`
}

type validateAccessTokenResp struct {
	IsValid bool   `json:"is_valid"`
	Err     string `json:"error"`
}

func (c *Conn) GenerateAccessToken(username, description string) (*AccessToken, error) {
	req := generateAccessTokenReq{
		Username:    username,
		Description: description,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, memphisError(err)
	}

	msg, err := c.brokerConn.Request(accessTokenGenerationSubject, b, 20*time.Second)
	if err != nil {
		return nil, memphisError(err)
	}

	ar := &generateAccessTokenResp{}
	err = json.Unmarshal(msg.Data, ar)
	if err != nil {
		return nil, defaultHandleCreationResp(msg.Data)
	}

	if ar.Err != "" {
		return nil, defaultHandleCreationResp([]byte(ar.Err))
	}

	return &AccessToken{
		AccessKeyID: ar.AccessKeyID,
		SecretKey:   ar.SecretKey,
	}, nil
}

func (c *Conn) ValidateAccessToken(accessKeyID, secretKey string) (bool, error) {
	req := validateAccessTokenReq{
		AccessKeyID: accessKeyID,
		SecretKey:   secretKey,
	}

	b, err := json.Marshal(req)
	if err != nil {
		return false, memphisError(err)
	}

	msg, err := c.brokerConn.Request(accessTokenValidationSubject, b, 20*time.Second)
	if err != nil {
		return false, memphisError(err)
	}

	vr := &validateAccessTokenResp{}
	err = json.Unmarshal(msg.Data, vr)
	if err != nil {
		return false, defaultHandleCreationResp(msg.Data)
	}

	if vr.Err != "" {
		return false, defaultHandleCreationResp([]byte(vr.Err))
	}

	return vr.IsValid, nil
}
