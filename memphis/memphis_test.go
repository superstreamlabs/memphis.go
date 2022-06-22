package memphis

import (
	"fmt"
	"testing"
)

func TestConnect(t *testing.T) {
	/* empty connection parameters */
	// host := ""
	// _, err := Connect(host, TcpPort(6011))

	// if err == nil {
	// 	t.Errorf("empty host should result with an error")
	// }

	got, err := Connect("localhost", Username("root"), ConnectionToken("memphis"))
	if err != nil {
		t.Error()
	}

	fmt.Println(got)
}
