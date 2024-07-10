<img src="/images/malogo.png" width="300" align="right" /><br><br><br>

# Introduction 
The Stream API is the component that manages the read and write of data into a Kafka broker as part of the Open Streaming Architecture. For more information, see [API Documentation](https://atlas.mclarenapplied.com/secu4/open_streaming_architecture/)

# Getting Started
## Client
For use with C#, NuGet packages are provided as part of the [McLaren Applied NuGet Repository](https://github.com/mat-docs/packages).

For other languages, you will need to use the proto files provided in the [MA.DataPlatforms.Protocol](https://github.com/Software-Products/MA.DataPlatforms.Protocol) repository.

Example usages can be found in the [Sample Code](https://github.com/mat-docs/MA.Streaming.Api.UsageSample) repository.

## Server
### Docker
The Stream API Server Docker image is provided in [DockerHub](https://hub.docker.com/r/mclarenapplied/streaming-proto-server-host).

To use the Stream API Server Docker image, use the following Docker Compose :

#### Docker Compose
```
services:  
  zookeeper:
    image: confluentinc/cp-zookeeper:latest    
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 12181:2181
    
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper 
    ports:
      - 9092:9092    
      - 9094:9094    
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9093,PLAINTEXT_HOST://kafka:9092,PLAINTEXT_LOCAL_HOST://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT_LOCAL_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  key-generator-service:
    image: mclarenapplied/keygenerator-proto-server:latest
    ports:    
      - 15379:15379
    environment:
      PORT: 15379

  stream-api-server:
    image: mclarenapplied/streaming-proto-server-host:latest
    ports:
      - 13579:13579
      - 10010:10010

    depends_on:
      - kafka
      - key-generator-service
    restart: always
    environment:     
      CONFIG_PATH: /Configs/AppConfig.json
      AUTO_START: true
    volumes:
      - ./Configs:/app/Configs 
```

### Standalone
It is possible to run the Stream API Server as a standalone executable or as a part of another application. In this case, the Stream API Docker image is not needed and can be replaced by the standalone executable.

### AppConfig.json
This json configures the Stream API Server and a sample json can look like this:
```
{
  "StreamCreationStrategy": 1,
  "BrokerUrl": "kafka:9092",
  "PartitionMappings": [
    {
      "Stream": "Stream1",
      "Partition": 1
    },
    {
      "Stream": "Stream2",
      "Partition": 2
    }
  ],
  "StreamApiPort": 13579,
  "IntegrateSessionManagement": true,
  "IntegrateDataFormatManagement": true,
  "UseRemoteKeyGenerator": true,
  "RemoteKeyGeneratorServiceAddress": "key-generator-service:15379",
  "BatchingResponses": false,
  "PrometheusMetricPort": 10010
}
```
