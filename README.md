<a href="![Github (4)](https://github.com/memphisdev/memphis-terraform/assets/107035359/a5fe5d0f-22e1-4445-957d-5ce4464e61b1)">![Github (4)](https://github.com/memphisdev/memphis-terraform/assets/107035359/a5fe5d0f-22e1-4445-957d-5ce4464e61b1)</a>
<p align="center">
<a href="https://memphis.dev/discord"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a>
<a href="https://github.com/memphisdev/memphis/issues?q=is%3Aissue+is%3Aclosed"><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis?color=6557ff"></a> 
  <img src="https://img.shields.io/npm/dw/memphis-dev?color=ffc633&label=installations">
<a href="https://github.com/memphisdev/memphis/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> 
<img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis?color=61dfc6">
<img src="https://img.shields.io/github/last-commit/memphisdev/memphis?color=61dfc6&label=last%20commit">
</p>

<div align="center">
  
  <img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/color/cncf-member-silver-color.svg#gh-light-mode-only">
  <img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/white/cncf-member-silver-white.svg#gh-dark-mode-only">
  
</div>
 
 <b><p align="center">
  <a href="https://memphis.dev/pricing/">Cloud</a> - <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">X</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p></b>

<div align="center">

  <h4>

**[Memphis.dev](https://memphis.dev)** is a highly scalable, painless, and effortless data streaming platform.<br>
Made to enable developers and data teams to collaborate and build<br>
real-time and streaming apps fast.

  </h4>
  
</div>

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
	memphis.ConnectionToken("<connection-token>"), // you will get it on application type user creation
	memphis.Password("<password>")) // depends on how Memphis deployed - default is connection token-based authentication
```
<br>
It is possible to pass connection configuration parameters, as function-parameters.

```go
// function params
c, err := memphis.Connect("<memphis-host>", 
	"<application type username>", 
	memphis.ConnectionToken("<connection-token>"), // you will get it on application type user creation
	memphis.Password("<password>"), // depends on how Memphis deployed - default is connection token-based authentication
  memphis.AccountId(<int>) // You can find it on the profile page in the Memphis UI. This field should be sent only on the cloud version of Memphis, otherwise it will be ignored
  memphis.Port(<int>), // defaults to 6666       
	memphis.Reconnect(<bool>), // defaults to true
	memphis.MaxReconnect(<int>), // defaults to 10
  memphis.ReconnectInterval(<time.Duration>) // defaults to 1 second
  memphis.Timeout(<time.Duration>) // defaults to 15 seconds
	// for TLS connection:
	memphis.Tls("<cert-client.pem>", "<key-client.pem>",  "<rootCA.pem>"),
	)
```

Once connected, all features offered by Memphis are available.<br>

### Disconnecting from Memphis
To disconnect from Memphis, call Close() on the Memphis connection object.<br>

```go
c.Close();
```

### Creating a Station
**Unexist stations will be created automatically through the SDK on the first producer/consumer connection with default values.**<br><br>
Stations can be created from Conn<br>
Passing optional parameters using functions<br>
_If a station already exists nothing happens, the new configuration will not be applied_<br>

```go
s0, err = c.CreateStation("<station-name>")

s1, err = c.CreateStation("<station-name>", 
 memphis.RetentionTypeOpt(<Messages/MaxMessageAgeSeconds/Bytes/AckBased>), // AckBased - cloud only
 memphis.RetentionVal(<int>), 
 memphis.StorageTypeOpt(<Memory/Disk>), 
 memphis.Replicas(<int>), 
 memphis.IdempotencyWindow(<time.Duration>), // defaults to 2 minutes
 memphis.SchemaName(<string>),
 memphis.SendPoisonMsgToDls(<bool>), // defaults to true
 memphis.SendSchemaFailedMsgToDls(<bool>), // defaults to true
 memphis.TieredStorageEnabled(<bool>), // defaults to false
 memphis.PartitionsNumber(<int>), // default is 1 partition
 memphis.DlsStation(<string>) // defaults to "" (no DLS station) - If selected DLS events will be sent to selected station as well
)
```

### Retention Types
Memphis currently supports the following types of retention:<br>

```go
memphis.MaxMessageAgeSeconds
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

```go
memphis.AckBased // for cloud users only
```

The above means that after a message is getting acked by all interested consumer groups it will be deleted from the Station.

### Retention Values

The `retention values` are directly related to the `retention types` mentioned above,<br> where the values vary according to the type of retention chosen.

All retention values are of type `int` but with different representations as follows:

`memphis.MaxMessageAgeSeconds` is represented **in seconds**, `memphis.Messages` in a **number of messages** <br> `memphis.Bytes` in a **number of bytes**, and finally `memphis.AckBased` is not using the retentionValue param at all. 

After these limits are reached oldest messages will be deleted.

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

### Creating a new Schema

```go
err := conn.CreateSchema("<schema-name>", "<schema-type>", "<schema-file-path>")
```

### Enforcing a Schema on an Existing Station

```go
err := conn.EnforceSchema("<schema-name>", "<station-name>")
```

### Deprecated - Attaching Schema
use EnforceSchema instead
```go
err := conn.AttachSchema("<schema-name>", "<station-name>")
```

### Detaching a Schema from Station

```go
err := conn.DetachSchema("<station-name>")
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
) 

// from a Station
p1, err := s.CreateProducer("<producer-name>")
```

### Producing a message
Without creating a producer (receiver function of the connection struct).
In cases where extra performance is needed the recommended way is to create a producer first
and produce messages by using the produce receiver function of it
```go
c.Produce("station_name_c_produce", "producer_name_a", []byte("Hey There!"), []memphis.ProducerOpt{}, []memphis.ProduceOpt{})
```

Creating a producer first (receiver function of the producer struct).
```go
p.Produce("<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>", memphis.AckWaitSec(15)) // defaults to 15 seconds
```
Note: 
When producing a message using avro format([]byte or map[string]interface{}), int types are converted to float64. Type conversion of `Golang float64` equals `Avro double`. So when creating an avro schema, it can't have int types. use double instead.
E.g.
```
myData :=  map[string]interface{}{
"username": "John",
"age": 30
}
```
```
{
	"type": "record",
	"namespace": "com.example",
	"name": "test_schema",
	"fields": [
		{ "name": "username", "type": "string" },
		{ "name": "age", "type": "double" }
	]
}
```
Note:
When producing to a station with more than one partition, the producer will produce messages in a Round Robin fashion between the different partitions.

### Add headers

```go
hdrs := memphis.Headers{}
hdrs.New()
err := hdrs.Add("key", "value")
p.Produce(
	"<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>",
    memphis.AckWaitSec(15),
	memphis.MsgHeaders(hdrs) // defaults to empty
)
```

### Async produce
For better performance. The client won't block requests while waiting for an acknowledgment.

```go
p.Produce(
	"<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>",
    memphis.AckWaitSec(15),
	memphis.AsyncProduce()
)
```

### Sync produce
For better reliability. The client will block requests and will wait for an acknowledgment.

```go
p.Produce(
	"<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>",
    memphis.AckWaitSec(15),
	memphis.SyncProduce()
)
```

### Message ID
Stations are idempotent by default for 2 minutes (can be configured), Idempotency achieved by adding a message id

```go
p.Produce(
	"<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>",
    memphis.AckWaitSec(15),
	memphis.MsgId("343")
)
```


### Produce using partition key
The partition key will be used to produce messages to a spacific partition.

```go
p.Produce(
	"<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>",
    memphis.ProducerPartitionKey(<string>)
)
```

### Produce using partition number
The partition number will be used to produce messages to a spacific partition.

```go
p.Produce(
	"<message in []byte or map[string]interface{}/[]byte or protoreflect.ProtoMessage or map[string]interface{}(schema validated station - protobuf)/struct with json tags or map[string]interface{} or interface{}(schema validated station - json schema) or []byte/string (schema validated station - graphql schema) or []byte or map[string]interface{} or struct with avro tags(schema validated station - avro schema)>",
    memphis.ProducerPartitionNumber(<int>)
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
  memphis.BatchSize(<batch-size> int), // defaults to 10
  memphis.BatchMaxWaitTime(<time.Duration>), // defaults to 5 seconds, has to be at least 1 ms
  memphis.MaxAckTime(<time.Duration>), // defaults to 30 sec
  memphis.MaxMsgDeliveries(<int>), // defaults to 10
  memphis.ConsumerErrorHandler(func(*Consumer, error){})
  memphis.StartConsumeFromSeq(<uint64>)// start consuming from a specific sequence. defaults to 1
  memphis.LastMessages(<int64>)// consume the last N messages, defaults to -1 (all messages in the station)
)
  
// creation from a Conn
consumer1, err = c.CreateConsumer("<station-name>", "<consumer-name>", ...) 
```
Note:
When consuming from a station with more than one partition, the consumer will consume messages in Round Robin fashion from the different partitions.

### Passing a context to a message handler

```go
ctx := context.Background()
ctx = context.WithValue(ctx, "key", "value")
consumer.SetContext(ctx)
```

### Processing Messages
First, create a callback function that receives a slice of pointers to ```memphis.Msg``` and an error.<br><br>
Then, pass this callback into ```consumer.Consume``` function.<br><br>
The consumer will try to fetch messages every ```pullInterval``` (that was given in Consumer's creation) and call the defined message handler.

```go
func handler(msgs []*memphis.Msg, err error, ctx context.Context) {
	if err != nil {
		fmt.Printf("Fetch failed: %v", err)
		return
	}

	for _, msg := range msgs {
		fmt.Println(string(msg.Data()))
		msg.Ack()
	}
}

consumer.Consume(handler, 
				memphis.ConsumerPartitionKey(<string>) // use the partition key to consume from a spacific partition (if not specified consume in a Round Robin fashion)
)
```

#### Consumer schema deserialization
To get messages deserialized, use `msg.DataDeserialized()`.  

```go
func handler(msgs []*memphis.Msg, err error, ctx context.Context) {
	if err != nil {
		fmt.Printf("Fetch failed: %v", err)
		return
	}

	for _, msg := range msgs {
		fmt.Println(string(msg.DataDeserialized()))
		msg.Ack()
	}
}
```

if you have ingested data into station in one format, afterwards you apply a schema on the station, the consumer won't deserialize the previously ingested data. For example, you have ingested string into the station and attached a protobuf schema on the station. In this case, consumer won't deserialize the string.

### Fetch a single batch of messages
```go
msgs, err := conn.FetchMessages("<station-name>", "<consumer-name>",
  memphis.FetchBatchSize(<int>) // defaults to 10
  memphis.FetchConsumerGroup("<consumer-group>"), // defaults to consumer name
  memphis.FetchBatchMaxWaitTime(<time.Duration>), // defaults to 5 seconds, has to be at least 1 ms
  memphis.FetchMaxAckTime(<time.Duration>), // defaults to 30 sec
  memphis.FetchMaxMsgDeliveries(<int>), // defaults to 10
  memphis.FetchConsumerErrorHandler(func(*Consumer, error){})
  memphis.FetchStartConsumeFromSeq(<uint64>)// start consuming from a specific sequence. defaults to 1
  memphis.FetchLastMessages(<int64>)// consume the last N messages, defaults to -1 (all messages in the station))
  memphis.FetchPartitionKey(<string>)// use the partition key to consume from a spacific partition (if not specified consume in a Round Robin fashion)
```

### Fetch a single batch of messages after creating a consumer
`prefetch = true` will prefetch next batch of messages and save it in memory for future Fetch() request<br>
Note: Use a higher MaxAckTime as the messages will sit in a local cache for some time before processing
```go
msgs, err := consumer.Fetch(<batch-size> int,
							<prefetch> bool,
							memphis.ConsumerPartitionKey(<string>) // use the partition key to consume from a spacific partition (if not specified consume in a Round Robin fashion)
							)
```

### Acknowledging a Message
Acknowledging a message indicates to the Memphis server to not <br>re-send the same message again to the same consumer or consumers group.

```shell
message.Ack();
```

### Delay the message after a given duration
Delay the message and tell Memphis server to re-send the same message again to the same consumer group. <br>The message will be redelivered only in case `Consumer.MaxMsgDeliveries` is not reached yet.

```go
message.Delay(<time.Duration>);
```

### Get headers 
Get headers per message
```go
headers := msg.GetHeaders()
```

### Get message sequence number
Get message sequence number
```go
sequenceNumber, err := msg.GetSequenceNumber()
```
### Destroying a Consumer

```go
consumer.Destroy();
```

### Check if broker is connected

```go
conn.IsConnected()
```
