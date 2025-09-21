package com.example.kafkaconsumer.service;

import com.example.kafkaconsumer.dto.MessageDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessagePublisherService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic.name}")
    private String topicName;

    @Value("${app.kafka.topic.retry-topic}")
    private String retryTopicName;

    @Value("${app.kafka.topic.dead-letter-topic}")
    private String deadLetterTopicName;

    public String getTopicName() {
        return topicName;
    }

    private ProducerRecord<String, Object> createProducerRecord(String topicName, Object messageDto) {
        String key = UUID.randomUUID().toString();
        return new ProducerRecord<>(topicName, key, messageDto);
    }

    /**
     * Publishes message to Kafka using KRaft mode (no Zookeeper dependency)
     */
    public void publishMessage(MessageDto messageDto) {
        try {
            log.info("Publishing message with ID: {} to Kafka topic: {} (KRaft mode)",
                    messageDto.getId(), topicName);

            // Create producer record with headers for better traceability
            ProducerRecord<String, Object> record = createProducerRecord(topicName, messageDto);

            // Send message asynchronously with callback
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

            // Handle success/failure with enhanced logging for KRaft
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info(
                            "Message published successfully to KRaft Kafka: {} -> Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                            messageDto.getId(), metadata.topic(), metadata.partition(),
                            metadata.offset(), metadata.timestamp());
                } else {
                    log.error("Failed to publish message to KRaft Kafka: {} -> Topic: {}, Error: {}",
                            messageDto.getId(), topicName, ex.getMessage(), ex);
                }
            });

        } catch (Exception e) {
            log.error("Exception while publishing message to KRaft Kafka: {} -> Topic: {}",
                    messageDto.getId(), topicName, e);
            throw new RuntimeException("Failed to publish message to KRaft Kafka", e);
        }
    }

    /**
     * Publishes message to custom topic with KRaft optimization
     */
    public void publishMessage(MessageDto messageDto, String customTopic) {
        try {
            log.info("Publishing message with ID: {} to custom KRaft topic: {}",
                    messageDto.getId(), customTopic);

            ProducerRecord<String, Object> record = createProducerRecord(customTopic, messageDto);

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("Message published to custom KRaft topic: {} -> Topic: {}, Partition: {}, Offset: {}",
                            messageDto.getId(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to publish to custom KRaft topic: {} -> Topic: {}",
                            messageDto.getId(), customTopic, ex);
                }
            });

        } catch (Exception e) {
            log.error("Exception publishing to custom KRaft topic: {} -> Topic: {}",
                    messageDto.getId(), customTopic, e);
            throw new RuntimeException("Failed to publish message to custom KRaft topic", e);
        }
    }

    /**
     * Publishes message to retry topic with enhanced retry logic for KRaft
     */
    public void publishToRetryTopic(MessageDto messageDto) {
        try {
            log.info("Publishing message with ID: {} to KRaft retry topic: {}",
                    messageDto.getId(), retryTopicName);

            // Increment retry count
            if (messageDto.getRetryCount() == null) {
                messageDto.setRetryCount(0);
            }
            messageDto.setRetryCount(messageDto.getRetryCount() + 1);

            // Add retry-specific headers
            ProducerRecord<String, Object> record = createProducerRecord(retryTopicName, messageDto);
            record.headers().add("retry-attempt",
                    String.valueOf(messageDto.getRetryCount()).getBytes(StandardCharsets.UTF_8));
            record.headers().add("original-topic", topicName.getBytes(StandardCharsets.UTF_8));
            record.headers().add("retry-timestamp", LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8));

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info(
                            "Message published to KRaft retry topic: {} -> Topic: {}, Partition: {}, Offset: {}, Retry: {}",
                            messageDto.getId(), metadata.topic(), metadata.partition(),
                            metadata.offset(), messageDto.getRetryCount());
                } else {
                    log.error("Failed to publish to KRaft retry topic: {} -> Topic: {}",
                            messageDto.getId(), retryTopicName, ex);
                }
            });

        } catch (Exception e) {
            log.error("Exception publishing to KRaft retry topic: {} -> Topic: {}",
                    messageDto.getId(), retryTopicName, e);
            throw new RuntimeException("Failed to publish message to KRaft retry topic", e);
        }
    }

    /**
     * Publishes message to dead letter topic with comprehensive metadata for KRaft
     */
    public void publishToDeadLetterTopic(MessageDto messageDto) {
        try {
            log.error("Publishing message with ID: {} to KRaft dead letter topic: {}",
                    messageDto.getId(), deadLetterTopicName);

            // Add DLT-specific headers for better debugging
            ProducerRecord<String, Object> record = createProducerRecord(deadLetterTopicName, messageDto);
            record.headers().add("dlt-reason", "max-retries-exceeded".getBytes(StandardCharsets.UTF_8));
            record.headers().add("original-topic", topicName.getBytes(StandardCharsets.UTF_8));
            record.headers().add("final-retry-count",
                    String.valueOf(messageDto.getRetryCount()).getBytes(StandardCharsets.UTF_8));
            record.headers().add("dlt-timestamp", LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8));

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.error("Message published to KRaft DLT: {} -> Topic: {}, Partition: {}, Offset: {}",
                            messageDto.getId(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to publish to KRaft DLT: {} -> Topic: {}",
                            messageDto.getId(), deadLetterTopicName, ex);
                }
            });

        } catch (Exception e) {
            log.error("Exception publishing to KRaft DLT: {} -> Topic: {}",
                    messageDto.getId(), deadLetterTopicName, e);
            throw new RuntimeException("Failed to publish message to KRaft dead letter topic", e);
        }
    }

    /**
     * Publishes message to specific partition (useful for ordered processing in
     * KRaft)
     */
    public void publishWithPartition(MessageDto messageDto, int partition) {
        try {
            log.info("Publishing message with ID: {} to Kafka topic: {}, partition: {}",
                    messageDto.getId(), topicName, partition);

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, partition,
                    messageDto.getId(), messageDto);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Message published to specific partition: {} -> topic: {}, partition: {}, offset: {}",
                            messageDto.getId(), result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to publish message: {} to topic: {}, partition: {}",
                            messageDto.getId(), topicName, partition, ex);
                }
            });

        } catch (Exception e) {
            log.error("Failed to publish message: {} to topic: {}, partition: {}",
                    messageDto.getId(), topicName, partition, e);
            throw new RuntimeException("Failed to publish message to specific partition", e);
        }
    }
}