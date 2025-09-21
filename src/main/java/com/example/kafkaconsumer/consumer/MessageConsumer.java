package com.example.kafkaconsumer.consumer;

import com.example.kafkaconsumer.dto.MessageDto;
import com.example.kafkaconsumer.service.MessageProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessageConsumer {

    private final MessageProcessingService messageProcessingService;

    @KafkaListener(topics = "${app.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessage(
            @Payload MessageDto messageDto,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            ConsumerRecord<String, MessageDto> consumerRecord,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Received message: {}", messageDto);
            log.debug("Topic: {}, Partition: {}, Offset: {}, Key: {}, Timestamp: {}", 
                     topic, partition, offset, key, timestamp);

            // Validate message
            if (messageDto == null || messageDto.getId() == null || messageDto.getContent() == null) {
                log.error("Invalid message received: {}", messageDto);
                throw new IllegalArgumentException("Message validation failed");
            }

            // Set Kafka metadata
            messageDto.setTopic(topic);
            messageDto.setPartition(partition);
            messageDto.setOffset(offset);

            // Create headers map
            Map<String, Object> headers = createHeadersMap(consumerRecord);

            // Process the message
            boolean processed = messageProcessingService.processMessage(messageDto, headers);
            
            if (processed) {
                log.info("Successfully processed message with ID: {} from topic: {}, partition: {}, offset: {}", 
                        messageDto.getId(), topic, partition, offset);
                // Manual acknowledgment if needed
                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
            } else {
                log.warn("Message processing failed for ID: {}", messageDto.getId());
                throw new RuntimeException("Message processing failed");
            }

        } catch (Exception e) {
            log.error("Error processing message from topic: {}, partition: {}, offset: {}, error: {}", 
                     topic, partition, offset, e.getMessage(), e);
            handleProcessingError(messageDto, e, consumerRecord);
            // Don't re-throw in Kafka to avoid infinite retries - let error handler deal with it
        }
    }

    @KafkaListener(topics = "${app.kafka.topic.retry-topic}", groupId = "${spring.kafka.consumer.group-id}-retry")
    public void consumeRetryMessage(
            @Payload MessageDto messageDto,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            ConsumerRecord<String, MessageDto> consumerRecord) {
        
        log.warn("Processing retry message from topic: {}, partition: {}, offset: {}, message: {}", 
                topic, partition, offset, messageDto);
        
        try {
            // Increment retry count
            if (messageDto.getRetryCount() == null) {
                messageDto.setRetryCount(0);
            }
            messageDto.setRetryCount(messageDto.getRetryCount() + 1);
            
            // Set Kafka metadata
            messageDto.setTopic(topic);
            messageDto.setPartition(partition);
            messageDto.setOffset(offset);
            
            Map<String, Object> headers = createHeadersMap(consumerRecord);
            
            // Retry processing
            boolean processed = messageProcessingService.processMessage(messageDto, headers);
            
            if (processed) {
                log.info("Successfully processed retry message with ID: {}", messageDto.getId());
            } else {
                log.error("Retry processing failed for message ID: {}", messageDto.getId());
                // Could send to dead letter topic here
            }
            
        } catch (Exception e) {
            log.error("Error processing retry message: {}", messageDto, e);
            // Could send to dead letter topic here
        }
    }

    @KafkaListener(topics = "${app.kafka.topic.dead-letter-topic}", groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void consumeDeadLetterMessage(
            @Payload MessageDto messageDto,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            ConsumerRecord<String, MessageDto> consumerRecord) {
        
        log.error("Processing message from Dead Letter Topic: {}, partition: {}, offset: {}, message: {}", 
                 topic, partition, offset, messageDto);
        
        // Set Kafka metadata
        messageDto.setTopic(topic);
        messageDto.setPartition(partition);
        messageDto.setOffset(offset);
        
        Map<String, Object> headers = createHeadersMap(consumerRecord);
        
        // Handle dead letter messages (could save to database, send alerts, etc.)
        messageProcessingService.handleDeadLetterMessage(messageDto, headers);
    }

    private Map<String, Object> createHeadersMap(ConsumerRecord<String, MessageDto> consumerRecord) {
        Map<String, Object> headers = new HashMap<>();
        
        // Add Kafka metadata
        headers.put("kafka.topic", consumerRecord.topic());
        headers.put("kafka.partition", consumerRecord.partition());
        headers.put("kafka.offset", consumerRecord.offset());
        headers.put("kafka.timestamp", consumerRecord.timestamp());
        headers.put("kafka.timestampType", consumerRecord.timestampType());
        headers.put("kafka.key", consumerRecord.key());
        
        // Add any custom headers from the record
        if (consumerRecord.headers() != null) {
            consumerRecord.headers().forEach(header -> {
                headers.put(header.key(), new String(header.value()));
            });
        }
        
        return headers;
    }

    private void handleProcessingError(MessageDto messageDto, Exception e, ConsumerRecord<String, MessageDto> consumerRecord) {
        log.error("Handling processing error for message ID: {}, Error: {}", 
                 messageDto != null ? messageDto.getId() : "unknown", e.getMessage());
        
        // Could implement custom error handling logic here
        // For example: send to retry topic, send to DLT, etc.
        
        if (messageDto != null && messageDto.getRetryCount() != null) {
            messageDto.setRetryCount(messageDto.getRetryCount() + 1);
        }
        
        // Log Kafka record details for debugging
        if (consumerRecord != null) {
            log.error("Failed record details - Topic: {}, Partition: {}, Offset: {}", 
                     consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        }
    }
}