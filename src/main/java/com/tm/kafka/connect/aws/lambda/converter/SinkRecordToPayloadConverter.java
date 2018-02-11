package com.tm.kafka.connect.aws.lambda.converter;

import org.apache.kafka.connect.sink.SinkRecord;

public interface SinkRecordToPayloadConverter {
  String convert(SinkRecord record) throws Exception;
}
