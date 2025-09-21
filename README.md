# Spring Boot Integration with Kafka v4

A Spring Boot helloworld style web application that publishes and consumes messages from Kafka v4. The application acts both as a publisher and as a consumer. Kafka runs in a docker container.

## Usage

```
1. Run Kafka v4:

podman run -d --name broker -p 9092:9092 apache/kafka:latest

2. Configure the topic name - my-message-topic in application.yml

3. Run the Spring Boot app:

.\gradlew bootRun

4. Publish a message:

curl -X POST "http://localhost:8080/api/messages/publish" -H "Content-Type: application/json" -d "{\"content\":\"KRaft test message\", \"type\":\"order\"}" -v

The server log should have:<br>
Message published successfully to KRaft Kafka: 1 -> Topic: my-message-topic, Partition: 0, Offset: 0, Timestamp: 1758483748928

4. Consume the message:

The Spring Boot app's KafkaListener should receive and echo the message like:

Successfully processed message with ID: 1 from topic: my-message-topic, partition: 0, offset: 0

```



