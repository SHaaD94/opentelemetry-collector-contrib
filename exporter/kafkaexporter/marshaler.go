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
	partitionTracesById := config.PartitionTracesByID
	if encoding == defaultEncoding {
		return newPdataTracesMarshaler(&ptrace.ProtoMarshaler{}, defaultEncoding, partitionTracesById), nil
	} else if encoding == "otlp_json" {
		return newPdataTracesMarshaler(&ptrace.JSONMarshaler{}, "otlp_json", partitionTracesById), nil
	} else if encoding == "zipkin_proto" {
		return newPdataTracesMarshaler(zipkinv2.NewProtobufTracesMarshaler(), "zipkin_proto", partitionTracesById), nil
	} else if encoding == "zipkin_json" {
		return newPdataTracesMarshaler(zipkinv2.NewJSONTracesMarshaler(), "zipkin_json", partitionTracesById), nil
	}

	// EZ FIXME check partitioning
	jaegerProto := jaegerMarshaler{marshaler: jaegerProtoSpanMarshaler{}}
	if encoding == jaegerProto.Encoding() {
		return jaegerProto, nil
	}
	jaegerJSON := jaegerMarshaler{marshaler: newJaegerJSONMarshaler()}
	if jaegerJSON.Encoding() == encoding {
		return jaegerJSON, nil
	}

	return nil, errUnrecognizedEncoding
}

// creates MetricsMarshaler based on the provided config
func createMetricMarshaller(config Config) (MetricsMarshaler, error) {
	encoding := config.Encoding
	partitionMetricsByResoures := config.PartitionMetricsByResourceAttributes
	if encoding == defaultEncoding {
		return newPdataMetricsMarshaler(&pmetric.ProtoMarshaler{}, defaultEncoding, partitionMetricsByResoures), nil
	} else if encoding == "otlp_json" {
		return newPdataMetricsMarshaler(&pmetric.JSONMarshaler{}, "otlp_json", partitionMetricsByResoures), nil
	}

	return nil, errUnrecognizedEncoding
}

// creates LogsMarshalers based on the provided config
func createLogMarshaler(config Config) (LogsMarshaler, error) {
	encoding := config.Encoding
	partitionLogsByAttributes := config.PartitionLogsByResourceAttributes
	if encoding == defaultEncoding {
		return newPdataLogsMarshaler(&plog.ProtoMarshaler{}, defaultEncoding, partitionLogsByAttributes), nil
	} else if encoding == "otlp_json" {
		return newPdataLogsMarshaler(&plog.JSONMarshaler{}, "otlp_json", partitionLogsByAttributes), nil
	}
	raw := newRawMarshaler()
	if encoding == raw.Encoding() {
		return raw, nil
	}
	return nil, errUnrecognizedEncoding
}
