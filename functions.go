package memphis

import (
	"context"
	"encoding/base64"

	"github.com/aws/aws-lambda-go/lambda"
)

type MemphisMsg struct {
	Headers map[string]string `json:"headers"`
	Payload string            `json:"payload"`
}

type MemphisMsgWithError struct {
	Headers map[string]string `json:"headers"`
	Payload string            `json:"payload"`
	Error   string            `json:"error"`
}

type MemphisEvent struct {
	Messages []MemphisMsg `json:"messages"`
}

type MemphisOutput struct {
	Messages       []MemphisMsg          `json:"messages"`
	FailedMessages []MemphisMsgWithError `json:"failed_messages"`
}

// EventHandlerFunction gets the message payload as []byte and message headers as map[string]string and should return the modified payload and headers.
// error should be returned if the message should be considered failed and go into the dead-letter station.
// if all returned values are nil the message will be filtered out of the station.
type EventHandlerFunction func([]byte, map[string]string) ([]byte, map[string]string, error)

// This function creates a Memphis function and processes events with the passed-in eventHandler function.
// eventHandlerFunction gets the message payload as []byte and message headers as map[string]string and should return the modified payload and headers.
// error should be returned if the message should be considered failed and go into the dead-letter station.
// if all returned values are nil the message will be filtered out from the station.
func CreateFunction(eventHandler EventHandlerFunction) {
	LambdaHandler := func(ctx context.Context, event *MemphisEvent) (*MemphisOutput, error) {
		var processedEvent MemphisOutput
		for _, msg := range event.Messages {
			payload, err := base64.StdEncoding.DecodeString(msg.Payload)
			if err != nil {
				processedEvent.FailedMessages = append(processedEvent.FailedMessages, MemphisMsgWithError{
					Headers: msg.Headers,
					Payload: msg.Payload,
					Error:   "couldn't decode message: " + err.Error(),
				})
				continue
			}

			modifiedPayload, modifiedHeaders, err := eventHandler(payload, msg.Headers)
			if err != nil {
				processedEvent.FailedMessages = append(processedEvent.FailedMessages, MemphisMsgWithError{
					Headers: msg.Headers,
					Payload: msg.Payload,
					Error:   err.Error(),
				})
				continue
			}

			if modifiedPayload != nil || modifiedHeaders != nil {
				modifiedPayloadStr := base64.StdEncoding.EncodeToString(modifiedPayload)
				processedEvent.Messages = append(processedEvent.Messages, MemphisMsg{
					Headers: modifiedHeaders,
					Payload: modifiedPayloadStr,
				})
			}
		}

		return &processedEvent, nil
	}

	lambda.Start(LambdaHandler)
}
