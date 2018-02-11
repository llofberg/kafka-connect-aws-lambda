package com.tm.kafka.connect.aws.lambda;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.google.gson.Gson;
import com.tm.kafka.connect.aws.lambda.converter.SinkRecordToPayloadConverter;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class AwsLambdaSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(AwsLambdaSinkTask.class);

  private AwsLambdaSinkConnectorConfig connectorConfig;
  private final Gson gson = new Gson();

  @FunctionalInterface
  interface AwsLambdaInvokeFunction<R, C, F, V> {
    V apply(R r, C c, F f);
  }

  AwsLambdaInvokeFunction<String, AWSCredentialsProvider, InvokeRequest, InvokeResult> invokeFunction =
    (region, credentialsProvider, request) ->
      AWSLambdaAsyncClientBuilder.standard()
        .withRegion(region)
        .withCredentials(credentialsProvider)
        .build().invoke(request);

  @Override
  public void start(Map<String, String> map) {
    connectorConfig = new AwsLambdaSinkConnectorConfig(map);
    context.timeout(connectorConfig.getRetryBackoff());
  }

  @Override
  public void stop() {
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    String awsFunctionName = connectorConfig.getAwsFunctionName();
    InvocationType awsLambdaInvocationType = connectorConfig.getAwsLambdaInvocationType();

    String awsRegion = connectorConfig.getAwsRegion();
    AWSCredentialsProvider credentialsProvider = connectorConfig.getAwsCredentialsProvider();
    SinkRecordToPayloadConverter sinkRecordToPayloadConverter = connectorConfig.getPayloadConverter();
    for (SinkRecord record : collection) {
      String payload = null;
      try {
        payload = sinkRecordToPayloadConverter.convert(record);
      } catch (Exception e) {
        throw new RetriableException(
          "Payload converter " + sinkRecordToPayloadConverter.getClass().getName() +
            " failed to convert '" + awsFunctionName + "' parameter. " + record.toString(), e);
      }

      InvokeRequest request = new InvokeRequest()
        .withFunctionName(awsFunctionName)
        .withInvocationType(awsLambdaInvocationType)
        .withPayload(payload);

      if (log.isTraceEnabled()) {
        log.trace("Calling {} with message: {}", awsFunctionName, payload);
      } else if (log.isDebugEnabled()) {
        log.debug("Calling {}", awsFunctionName);
      }

      try {
        invokeFunction.apply(awsRegion, credentialsProvider, request);
      } catch (Exception e) {
        throw new RetriableException("AWS Lambda function '" + awsFunctionName + "' invocation failed. " + payload, e);
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("Read {} records from Kafka", collection.size());
    }
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

}
