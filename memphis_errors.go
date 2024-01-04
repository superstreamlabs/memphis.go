package memphis

import (
	"fmt"
	"strconv"
	"errors"
)

var (
	errInvalidConnectionType = memphisError(errors.New("you have to connect with one of the following methods: connection token / password"))
	errMissingTLSCertFile = memphisError(errors.New("must provide a TLS cert file"))
	errMissingTLSKeyFile = memphisError(errors.New("must provide a TLS key file"))
	errMissingTLSCaFile = memphisError(errors.New("must provide a TLS ca file"))
	errPartitionNumOutOfRange = memphisError(errors.New("partition number is out of range"))
	errConsumerErrStationUnreachable = memphisError(errors.New("station unreachable"))
	errConsumerErrConsumeInactive    = memphisError(errors.New("consumer is inactive"))
	errConsumerErrDelayDlsMsg        = memphisError(errors.New("cannot delay DLS message"))
	errInvalidMessageFormat = memphisError(errors.New("message format is not supported"))
	errExpectingProtobuf = memphisError(errors.New("invalid message format, expecting protobuf"))
	errBothPartitionNumAndKey = memphisError(errors.New("can not use both partition number and partition key"))
	errStartConsumeNotPositive = memphisError(errors.New("startConsumeFromSequence has to be a positive number"))
	errLastMessagesNegative = memphisError(errors.New("min value for LastMessages is -1"))
	errBothStartConsumeAndLastMessages = memphisError(errors.New("Consumer creation options can't contain both startConsumeFromSequence and lastMessages"))
	errUnreachableStation = memphisError(errors.New("station unreachable"))
	errInvalidStationName = memphisError(errors.New("station name should be either string or []string"))
	errInvalidHeaderKey = memphisError(errors.New("keys in headers should not start with $memphis"))
	errUnsupportedMsgType = memphisError(errors.New("unsupported message type"))
	errEmptyMsgId = memphisError(errors.New("msg id can not be empty"))
	errPartitionNotInKey = memphisError(errors.New("failed to get partition from key"))
	errMissingFunctionsListener = memphisError(errors.New("functions listener doesn't exist"))
	errMissingSchemaListener = memphisError(errors.New("schema listener doesn't exist"))
	errStationNotSubedToSchema = memphisError(errors.New("station subscription doesn't exist"))
	errInvalidSchmeaType = memphisError(errors.New("invalid schema type"))
	errExpectinGraphQL = memphisError(errors.New("invalid message format, expecting GraphQL"))
)

func errInvalidAvroFormat(err error) error{
	return memphisError(errors.New("Bad Avro format - " + err.Error()))
}

func errProducerNotInCache(producerName string) error{
	return memphisError(fmt.Errorf("%s not exists on the map", producerName))
}

func errLoadClientCertFailed(err error) error{
	return memphisError(errors.New("memphis: error loading client certificate: " + err.Error()))
}

func errInvalidBatchSize(maxBatchSize int) error{
	return memphisError(errors.New("Batch size can not be greater than " + strconv.Itoa(maxBatchSize) + " or less than 1"))
}

func errPartitionNotInStation(partitionNumber int, stationName string) error {
	return memphisError(fmt.Errorf("partition %v does not exist in station %v", partitionNumber, stationName))
}

func errSchemaValidationFailed(err error) error {
	return memphisError(errors.New("Schema validation has failed: " + err.Error()))
}

func errMessageMisalignedSchema(err error) error {
	return memphisError(errors.New("Deserialization has been failed since the message format does not align with the currently attached schema: " + err.Error()))
}

func errBadJSON(err error) error {
	return memphisError(errors.New("Bad JSON format - " + err.Error()))
}