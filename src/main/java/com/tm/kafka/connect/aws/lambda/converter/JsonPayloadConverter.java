package com.tm.kafka.connect.aws.lambda.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class JsonPayloadConverter implements SinkRecordToPayloadConverter {
  private Logger log = LoggerFactory.getLogger(JsonPayloadConverter.class);
  private ObjectMapper objectMapper = new ObjectMapper();

  public String convert(SinkRecord record) throws JsonProcessingException {
    Object value = record.value();
    Schema schema = record.valueSchema();
    String topic = record.topic();

    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(new HashMap<>(), false);
    byte[] a = jsonConverter.fromConnectData(topic, schema, value);
    JsonDeserializer jsonDeserializer = new JsonDeserializer();
    jsonDeserializer.configure(new HashMap<>(), false);
    JsonNode b = jsonDeserializer.deserialize(record.topic(), a);

    String payload = objectMapper.writeValueAsString(b);

    log.trace("P: {}", payload);

    return payload;
  }
}
