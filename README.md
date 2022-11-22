<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis-broker/blob/master/logo-white.png?raw=true#gh-dark-mode-only)
  
</div>

<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis-broker/blob/master/logo-black.png?raw=true#gh-light-mode-only)
  
</div>

<div align="center">
<h1>Real-Time Data Processing Platform</h1>

<img width="750" alt="Memphis UI" src="https://user-images.githubusercontent.com/70286779/182241744-2016dc1a-c758-48ba-8666-40b883242ea9.png">


<a target="_blank" href="https://twitter.com/intent/tweet?text=Probably+The+Easiest+Message+Broker+In+The+World%21+%0D%0Ahttps%3A%2F%2Fgithub.com%2Fmemphisdev%2Fmemphis-broker+%0D%0A%0D%0A%23MemphisDev"><img src="https://user-images.githubusercontent.com/70286779/174467733-e7656c1e-cfeb-4877-a5f3-1bd4fccc8cf1.png" width="60"></a> 
</div>
 
 <p align="center">
  <a href="https://demo.memphis.dev/">Playground</a> - <a href="https://sandbox.memphis.dev/" target="_blank">Sandbox</a> - <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">Twitter</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p>

<p align="center">
<a href="https://discord.gg/WZpysvAeTf"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a> <a href=""><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis-broker?color=6557ff"></a> <a href="https://github.com/memphisdev/memphis-broker/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> <a href="https://github.com/memphisdev/memphis-broker/blob/master/LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg"></a> <img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis-broker?color=61dfc6"> <img src="https://img.shields.io/github/last-commit/memphisdev/memphis-broker?color=61dfc6&label=last%20commit">
</p>

**[Memphis{dev}](https://memphis.dev)** is an open-source real-time data processing platform<br>
that provides end-to-end support for in-app streaming use cases using Memphis distributed message broker.<br>
Memphis' platform requires zero ops, enables rapid development, extreme cost reduction, <br>
eliminates coding barriers, and saves a great amount of dev time for data-oriented developers and data engineers.

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
	Port(<int>),        
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

### Creating a Station
Stations can be created from Conn<br>
Passing optional parameters using functions<br>

```go
s0, err = c.CreateStation("<station-name>")

s1, err = c.CreateStation("<station-name>", 
 RetentionTypeOpt(<Messages/MaxMeMessageAgeSeconds/Bytes>),
 RetentionVal(<int>), 
 StorageTypeOpt(<Memory/Disk>), 
 Replicas(<int>), 
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
memphis.Disk
```

The above means that messages persist on disk.

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
p0, err := c.CreateProducer(
	"<station-name>",
	"<producer-name>",
	memphis.ProducerGenUniqueSuffix()
) 

// from a Station
p1, err := s.CreateProducer("<producer-name>")
```

### Producing a message

```go
p.Produce("<message in []byte/protoreflect.ProtoMessage/json schema in case it is a schema validated station>", memphis.AckWaitSec(15)) // defaults to 15 seconds
```

### Add headers

```go
hdrs := memphis.Headers{}
hdrs.New()
err := hdrs.Add("key", "value")
p.Produce(
	"<message in []byte>/protoreflect.ProtoMessage/json schema in case it is a schema validated station",
    memphis.AckWaitSec(15),
	memphis.MsgHeaders(hdrs) // defaults to empty
)
```

### Async produce
Meaning your application won't wait for broker acknowledgement - use only in case you are tolerant for data loss

```go
p.Produce(
	"<message in []byte>/protoreflect.ProtoMessage/json schema in case it is a schema validated station",
    memphis.AckWaitSec(15),
	memphis.AsyncProduce()
)
```

### Destroying a Producer

```go
p.Destroy();
```

### Creating a Consumer

```go
// creation from a Station
consumer0, err = s.CreateConsumer("<consumer-name>",
  memphis.ConsumerGroup("<consumer-group>"), // defaults to consumer name
  memphis.PullInterval(<pull interval time.Duration), // defaults to 1 second
  memphis.BatchSize(<batch-size int), // defaults to 10
  memphis.BatchMaxWaitTime(<time.Duration>), // defaults to 5 seconds, has to be at least 1 ms
  memphis.MaxAckTime(<time.Duration>), // defaults to 30 sec
  memphis.MaxMsgDeliveries(<int>), // defaults to 10
  memphis.ConsumerGenUniqueSuffix(),
  memphis.ConsumerErrorHandler(func(*Consumer, error){})
)
  
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

### Get headers 
Get headers per message
```go
headers := msg.GetHeaders()
```
### Destroying a Consumer

```shell
consumer.Destroy();
```
