<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis-broker/blob/staging/logo-white.png?raw=true#gh-dark-mode-only)
  
</div>

<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis-broker/blob/staging/logo-black.png?raw=true#gh-light-mode-only)
  
</div>

<div align="center">
<h1>A Powerful Messaging Platform For Devs</h1>
<a target="_blank" href="https://twitter.com/intent/tweet?text=Probably+The+Easiest+Message+Broker+In+The+World%21+%0D%0Ahttps%3A%2F%2Fgithub.com%2Fmemphisdev%2Fmemphis-broker+%0D%0A%0D%0A%23MemphisDev"><img src="https://user-images.githubusercontent.com/70286779/174467733-e7656c1e-cfeb-4877-a5f3-1bd4fccc8cf1.png" width="60"></a> 
</div>
 
 <p align="center">
  <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">Twitter</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p>

<p align="center">
<a href="https://discord.gg/WZpysvAeTf"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a> <a href=""><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis-broker?color=6557ff"></a> <a href="https://github.com/memphisdev/memphis-broker/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> <a href="https://github.com/memphisdev/memphis-broker/blob/master/LICENSE"><img src="https://img.shields.io/github/license/memphisdev/memphis-broker?color=ffc633" alt="License"></a> <img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis-broker?color=61dfc6"> <img src="https://img.shields.io/github/last-commit/memphisdev/memphis-broker?color=61dfc6&label=last%20commit"> <img src="https://goreportcard.com/badge/github.com/memphisdev/memphis.go">
</p>

**[Memphis{dev}](https://memphis.dev)** is a Go-based message broker for developers made out of devs' struggles develop around message brokers.<br>Enables devs to achieve all other message brokers' benefits in a fraction of the time.<br>
Focusing on automatic optimization, schema management, inline processing,  and troubleshooting abilities. All under the same hood.
Utilizing NATS core.

# Installation
After installing and running memphis broker,<br>
In your project's directory:

```shell
go get github.com/memphisdev/memphis.go
```

# Importing
```go
import "github.com/memphisdev/memphis.go"
```

### Connecting to Memphis
```go
c, err := memphis.Connect("<memphis-host>", 
	"<application type username>", 
	"<broker-token>")
```
<br>
It is possible to pass connection configuration parameters, as function-parameters.

```go
// function params
c, err := memphis.Connect("<memphis-host>", 
	"<application type username>", 
	"<broker-token>",
	ManagementPort(<int>),
	TcpPort(<int>),
	DataPort(<int>),        
	Reconnect(<bool>),
	MaxReconnect(<int>)
	)
```

Once connected, all features offered by Memphis are available.<br>

### Disconnecting from Memphis
To disconnect from Memphis, call Close() on the Memphis connection object.<br>

```go
c.Close();
```

### Creating a Factory

```go
// c is of type memphis.Conn
f, err := c.CreateFactory("<factory-name>", 
    Description("<optional-description>")
```

### Destroying a Factory
Destroying a factory will remove all its resources (including stations, producers, and consumers).<br>

```go
err := f.Destroy()
```

### Creating a Station
Stations can be created from both Conn and Factory<br>
Passing optional parameters using functions<br>

```go
s0, err = c.CreateStation("<station-name>","<factory-name>")

s1, err = c.CreateStation("<station-name>", 
"<factory-name>",
 RetentionTypeOpt(<Messages/MaxMeMessageAgeSeconds/Bytes>),
 RetentionVal(<int>), 
 StorageTypeOpt(<Memory/File>), 
 Replicas(<int>), 
 EnableDedup(), 
 DedupWindow(<time.Duration>))
 
s2, err = f.CreateStation("<station-name>", 
 RetentionTypeOpt(<Messages/MaxMeMessageAgeSeconds/Bytes>),
 RetentionVal(<retention-value>), 
 StorageTypeOpt(<Memory/File>), 
 Replicas(<intgo>), 
 EnableDedup(), 
 DedupWindow(<time.Duration>))
```

### Retention Types
Memphis currently supports the following types of retention:<br>

```go
memphis.MaxMeMessageAgeSeconds
```

The above means that every message persists for the value set in the retention value field (in seconds).

```go
memphis.Messages
```

The above means that after the maximum number of saved messages (set in retention value)<br>has been reached, the oldest messages will be deleted.

```go
memphis.Bytes
```

The above means that after maximum number of saved bytes (set in retention value)<br>has been reached, the oldest messages will be deleted.

### Storage Types
Memphis currently supports the following types of messages storage:<br>

```go
memphis.File
```

The above means that messages persist on the file system.

```go
memphis.Memory
```

The above means that messages persist on the main memory.<br>

### Destroying a Station
Destroying a station will remove all its resources (including producers and consumers).<br>

```go
err := s.Destroy();
```

### Produce and Consume Messages
The most common client operations are producing messages and consuming messages.<br><br>
Messages are published to a station and consumed from it<br>by creating a consumer and calling its Consume function with a message handler callback function.<br>Consumers are pull-based and consume all the messages in a station<br> unless you are using a consumers group,<br>in which case messages are spread across all members in this group.<br><br>
Memphis messages are payload agnostic. Payloads are byte slices, i.e []byte.<br><br>
In order to stop receiving messages, you have to call ```consumer.StopConsume()```.<br>The consumer will terminate regardless of whether there are messages in flight for the client.

### Creating a Producer

```go
// from a Conn
p0, err := c.CreateProducer("<station-name>", "<producer-name>") 

// from a Station
p1, err := s.CreateProducer("<producer-name>")
```

### Producing a Message

```go
p.Produce("<message in []byte>",
            ackWait(<ack time.Duration>)) // defaults to 15 seconds
```

### Destroying a Producer

```go
p.Destroy();
```

### Creating a Consumer

```go
// creation from a Station
consumer0, err = s.CreateConsumer("<consumer-name>",
  ConsumerGroup("<consumer-group>"), // defaults to consumer name
  PullInterval(<pull interval time.Duration), // defaults to 1 second
  BatchSize(<batch-size int), // defaults to 10
  BatchMaxWaitTime(<time.Duration>), // defaults to 5 seconds
  MaxAckTime(<time.Duration>), // defaults to 30 sec
  MaxMsgDeliveries(<int>)) // defaults to 10
  
// creation from a Conn
consumer1, err = c.CreateConsumer("<station-name>", "<consumer-name>", ...) 
```

### Processing Messages
First, create a callback function that receives a slice of pointers to ```memphis.Msg``` and an error.<br><br>
Then, pass this callback into ```consumer.Consume``` function.<br><br>
The consumer will try to fetch messages every ```pullInterval``` (that was given in Consumer's creation) and call the defined message handler.

```go
func handler(msgs []*memphis.Msg, err error) {
	if err != nil {
		m := msgs[0]
		fmt.Println(string(m.Data()))
		m.Ack()
	}
}

consumer.Consume(handler)
```

You can trigger a single fetch with the Fetch() method

```shell
msgs, err := consumer.Fetch()
```

### Acknowledging a Message
Acknowledging a message indicates to the Memphis server to not <br>re-send the same message again to the same consumer or consumers group.

```shell
message.Ack();
```

### Destroying a Consumer

```shell
consumer.Destroy();
```
