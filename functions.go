package memphis

import (
	"context"
)

type MemphisMsg struct {
	Headers map[string]string `json:"headers"`
	Payload []byte            `json:"payload"`
}

type MemphisMsgWithError struct{
	Headers map[string]string `json:"headers"`
	Payload []byte            `json:"payload"`
	Error string			  `json:"error"`
}

type MemphisEvent struct {
	Messages []MemphisMsg `json:"messages"`
	FailedMessages []MemphisMsgWithError `json:"failedMessages"`
}

type UserFunction func([]byte) ([]byte, error)

func create_function(userFunction UserFunction) func(context.Context, *MemphisEvent)(*MemphisEvent, error){
	LambdaHandler := func(ctx context.Context, event *MemphisEvent) (*MemphisEvent, error) {
		var processedEvent MemphisEvent
		for _, msg := range event.Messages {	
			modifiedPayload, err := userFunction(msg.Payload)
	
			if err != nil{
				processedEvent.FailedMessages = append(processedEvent.FailedMessages, MemphisMsgWithError{
					Headers: msg.Headers,
					Payload: msg.Payload,
					Error: err.Error(),
				})
	
				continue
			}
	
			processedEvent.Messages = append(processedEvent.Messages, MemphisMsg{
				Headers: msg.Headers,
				Payload: modifiedPayload,
			})
		}
	
		return &processedEvent, nil
	}

	return LambdaHandler
}

