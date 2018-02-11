Kafka Connect AWS Lambda connector
==================================

# Running in development

The [docker-compose.yml](docker-compose.yml) that is included in this repository is based on the Confluent Platform Docker
images. Take a look at the [quickstart](http://docs.confluent.io/3.0.1/cp-docker-images/docs/quickstart.html#getting-started-with-docker-client)
for the Docker images. 

Examples of the example payload converters:

```
docker-compose up -d
```

With plain json messages:

```
# docker exec -it kafkaconnectawslambda_kafka_1 bash
# cd /data
# ./bin/debug.sh config/connect-json-docker.properties config/AwsLambdaSinkConnector.properties
```

```
# kafka-console-producer --broker-list kafka:9092 --topic aws-lambda-topic --property parse.key='true' --property key.separator=':'
> K2:{"f1":"A7"}
```

With JsonPayloadConverter the Lambda function sees:

```
{ schema: null, payload: { f1: 'A7' } }
```

With DefaultPayloadConverter the Lambda function sees:

```
{ kafkaOffset: 34,
timestampType: 'CREATE_TIME',
topic: 'aws-lambda-topic',
kafkaPartition: 0,
keySchema: { type: 'STRING', optional: true },
key: 'K2',
value: { f1: 'A7' },
timestamp: 1518315606220 }
```

With schema-registry and Avro messages:
```
# docker exec -it kafkaconnectawslambda_kafka_1 bash
# cd /data
# ./bin/debug.sh config/connect-avro-docker.properties config/AwsLambdaSinkConnector.properties
```

```
# kafka-avro-console-producer --broker-list kafka:9092 --topic aws-lambda-topic --property value.schema='{"type":"record","name":"test","fields":[{"name":"f1","type":"string"}]}' --property sc\
hema.registry.url='http://schema_registry:8081/'
{"f1":"v1"}
```

With JsonPayloadConverter the Lambda function sees:

```
{ schema: 
{ type: 'struct',
fields: [ [Object] ],
optional: false,
name: 'test',
version: 1 },
payload: { f1: 'AZ' } }
```


With DefaultPayloadConverter the Lambda function sees:

```
{ kafkaOffset: 35,
   timestampType: 'CREATE_TIME',
   topic: 'aws-lambda-topic',
   kafkaPartition: 0,
   keySchema: { type: 'STRING', optional: true },
   valueSchema: 
   { type: 'STRUCT',
   optional: false,
   fields: [ [Object] ],
   fieldsByName: { f1: [Object] },
   name: 'test',
   version: 1 },
   value: 
   { schema: 
   { type: 'STRUCT',
   optional: false,
   fields: [Object],
   fieldsByName: [Object],
   name: 'test',
   version: 1 },
   values: [ 'AZ' ] },
   timestamp: 1518315749655 }
```

Start the connector with debugging enabled. This will wait for a debugger to attach.

```
export SUSPEND='y'
./bin/debug.sh config/connect-json-docker.properties config/AwsLambdaSinkConnector.properties
```

