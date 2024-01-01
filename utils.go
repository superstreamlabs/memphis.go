package memphis

import (
	"errors"
	"strings"
)

func memphisError(err error) error {
	if err == nil {
		return nil
	}
	message := strings.Replace(err.Error(), "nats", "memphis", -1)

	if strings.Contains(message, "Permissions Violation") {
		return errors.New("memphis: Permissions Violation")
	}
	return errors.New(message)
}
