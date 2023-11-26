package memphis

import (
	"embed"
	"os"
	"testing"
	"time"

	"github.com/g41797/sputnik/sidecar"
	"github.com/nats-io/nats.go"
)

//go:embed _configuration
var embconf embed.FS

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

// second level function, because os.Exit does not honor defer
func testMain(m *testing.M) int {

	clean, err := prepareTestEnvironment(&embconf)
	defer clean()

	if err != nil {
		return -1
	}

	return m.Run()
}

// Ensure running tests in ready environment
func prepareTestEnvironment(embconf *embed.FS) (cleanFunc func(), err error) {

	cleaner := new(sidecar.Cleaner)

	// Create configuration and env files from embedded ones
	cleanUp, err := sidecar.UseEmbeddedConfiguration(embconf)
	if err != nil {
		return cleaner.Clean, err
	}

	cleaner.Push(cleanUp)

	// Setup environment variables from env files
	// Replaces vscode setting in launch.json file
	// Reason - running tests in github without vscode
	err = sidecar.LoadEnv()
	if err != nil {
		return cleaner.Clean, err
	}

	composeurl, exists := os.LookupEnv("COMPOSEURL")

	if !exists {
		composeurl = os.Getenv("DEV_COMPOSEURL")
	}

	if err = sidecar.LoadDockerComposeFile(composeurl); err != nil {
		return cleaner.Clean, err
	}

	// Start broker and additional servers using docker compose file
	stop, err := sidecar.StartServices()
	if err != nil {
		return cleaner.Clean, err
	}
	cleaner.Push(stop)

	conn := waitStart()
	cleaner.Push(func() { conn.Close() })

	return cleaner.Clean, nil

}

func waitStart() *nats.Conn {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			conn, err := connect()
			if err == nil {
				ticker.Stop()
				return conn
			}
		}
	}
}

func connect() (*nats.Conn, error) {

	url := "localhost:6666"
	username := "root"
	creds := "memphis"
	name := "MEMPHIS HTTP LOGGER"

	var nc *nats.Conn
	var err error

	natsOpts := nats.Options{
		Url:            url,
		AllowReconnect: true,
		MaxReconnect:   10,
		ReconnectWait:  3 * time.Second,
		Name:           name,
		Password:       creds,
		User:           username,
	}

	nc, err = natsOpts.Connect()
	if err != nil {
		return nil, err
	}

	return nc, nil
}
