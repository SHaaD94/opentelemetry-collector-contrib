// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/zipkin/zipkinv2"
)

// TracesMarshaler marshals traces into Message array.
type TracesMarshaler interface {
	// Marshal serializes spans into sarama's ProducerMessages
	Marshal(traces ptrace.Traces, topic string) ([]*sarama.ProducerMessage, error)

	// Encoding returns encoding name
	Encoding() string
}

// MetricsMarshaler marshals metrics into Message array
type MetricsMarshaler interface {
	// Marshal serializes metrics into sarama's ProducerMessages
	Marshal(metrics pmetric.Metrics, topic string) ([]*sarama.ProducerMessage, error)

	// Encoding returns encoding name
	Encoding() string
}

// LogsMarshaler marshals logs into Message array
type LogsMarshaler interface {
	// Marshal serializes logs into sarama's ProducerMessages
	Marshal(logs plog.Logs, topic string) ([]*sarama.ProducerMessage, error)

	// Encoding returns encoding name
	Encoding() string
}

// creates TracesMarshaller based on the provided config
func createTracesMarshaler(config Config) (TracesMarshaler, error) {
	encoding := config.Encoding
	partitionTracesByID := config.PartitionTracesByID

	jaegerProto := jaegerMarshaler{marshaler: jaegerProtoSpanMarshaler{}}
	jaegerJSON := jaegerMarshaler{marshaler: newJaegerJSONMarshaler()}

	switch encoding {
	case defaultEncoding:
		return newPdataTracesMarshaler(&ptrace.ProtoMarshaler{}, defaultEncoding, partitionTracesByID), nil
	case "otlp_json":
		return newPdataTracesMarshaler(&ptrace.JSONMarshaler{}, "otlp_json", partitionTracesByID), nil
	case "zipkin_proto":
		return newPdataTracesMarshaler(zipkinv2.NewProtobufTracesMarshaler(), "zipkin_proto", partitionTracesByID), nil
	case "zipkin_json":
		return newPdataTracesMarshaler(zipkinv2.NewJSONTracesMarshaler(), "zipkin_json", partitionTracesByID), nil
	case jaegerProtoSpanMarshaler{}.encoding():
		return jaegerProto, nil
	case jaegerJSON.Encoding():
		return jaegerJSON, nil
	default:
		return nil, errUnrecognizedEncoding
	}

}

// creates MetricsMarshaler based on the provided config
func createMetricMarshaller(config Config) (MetricsMarshaler, error) {
	encoding := config.Encoding
	partitionMetricsByResoures := config.PartitionMetricsByResourceAttributes
	switch encoding {
	case defaultEncoding:
		return newPdataMetricsMarshaler(&pmetric.ProtoMarshaler{}, defaultEncoding, partitionMetricsByResoures), nil
	case "otlp_json":
		return newPdataMetricsMarshaler(&pmetric.JSONMarshaler{}, "otlp_json", partitionMetricsByResoures), nil
	default:
		return nil, errUnrecognizedEncoding
	}
}

// creates LogsMarshalers based on the provided config
func createLogMarshaler(config Config) (LogsMarshaler, error) {
	encoding := config.Encoding
	partitionLogsByAttributes := config.PartitionLogsByResourceAttributes

	raw := newRawMarshaler()
	switch encoding {
	case defaultEncoding:
		return newPdataLogsMarshaler(&plog.ProtoMarshaler{}, defaultEncoding, partitionLogsByAttributes), nil
	case "otlp_json":
		return newPdataLogsMarshaler(&plog.JSONMarshaler{}, "otlp_json", partitionLogsByAttributes), nil
	case raw.Encoding():
		return raw, nil
	default:
		return nil, errUnrecognizedEncoding
	}
}
