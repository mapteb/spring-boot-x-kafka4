package com.example.kafkaconsumer.service;

import com.example.kafkaconsumer.dto.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class MessageProcessingService {

    private final Map<String, MessageDto> processedMessages = new ConcurrentHashMap<>();
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong deadLetterCount = new AtomicLong(0);

    public boolean processMessage(MessageDto messageDto, Map<String, Object> headers) {
        log.info("Processing message of type '{}' with ID: {} from Kafka topic: {}, partition: {}, offset: {}", 
                messageDto.getType(), messageDto.getId(), messageDto.getTopic(), 
                messageDto.getPartition(), messageDto.getOffset());
        
        try {
            // Simulate processing based on message type
            switch (messageDto.getType().toLowerCase()) {
                case "order":
                    return processOrderMessage(messageDto);
                case "payment":
                    return processPaymentMessage(messageDto);
                case "notification":
                    return processNotificationMessage(messageDto);
                case "user":
                    return processUserMessage(messageDto);
                default:
                    return processGenericMessage(messageDto);
            }
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
            failedCount.incrementAndGet();
            return false;
        }
    }

    private boolean processOrderMessage(MessageDto messageDto) {
        log.info("Processing order message: {}", messageDto.getContent());
        
        // Simulate order processing logic
        if (messageDto.getContent().contains("invalid")) {
            throw new RuntimeException("Invalid order data");
        }
        
        // Simulate some processing time
        simulateProcessingDelay(500);
        
        processedMessages.put(messageDto.getId(), messageDto);
        processedCount.incrementAndGet();
        log.info("Order processed successfully for ID: {} from Kafka offset: {}", 
                messageDto.getId(), messageDto.getOffset());
        return true;
    }

    private boolean processPaymentMessage(MessageDto messageDto) {
        log.info("Processing payment message: {}", messageDto.getContent());
        
        // Simulate payment processing logic
        if (messageDto.getContent().contains("declined")) {
            throw new RuntimeException("Payment declined");
        }
        
        simulateProcessingDelay(300);
        
        processedMessages.put(messageDto.getId(), messageDto);
        processedCount.incrementAndGet();
        log.info("Payment processed successfully for ID: {} from Kafka offset: {}", 
                messageDto.getId(), messageDto.getOffset());
        return true;
    }

    private boolean processNotificationMessage(MessageDto messageDto) {
        log.info("Processing notification message: {}", messageDto.getContent());
        
        // Simulate notification sending
        simulateProcessingDelay(100);
        
        processedMessages.put(messageDto.getId(), messageDto);
        processedCount.incrementAndGet();
        log.info("Notification sent successfully for ID: {} from Kafka offset: {}", 
                messageDto.getId(), messageDto.getOffset());
        return true;
    }

    private boolean processUserMessage(MessageDto messageDto) {
        log.info("Processing user message: {}", messageDto.getContent());
        
        // Simulate user data processing
        simulateProcessingDelay(200);
        
        processedMessages.put(messageDto.getId(), messageDto);
        processedCount.incrementAndGet();
        log.info("User message processed successfully for ID: {} from Kafka offset: {}", 
                messageDto.getId(), messageDto.getOffset());
        return true;
    }

    private boolean processGenericMessage(MessageDto messageDto) {
        log.info("Processing generic message: {}", messageDto.getContent());
        
        simulateProcessingDelay(150);
        
        processedMessages.put(messageDto.getId(), messageDto);
        processedCount.incrementAndGet();
        log.info("Generic message processed successfully for ID: {} from Kafka offset: {}", 
                messageDto.getId(), messageDto.getOffset());
        return true;
    }

    public void handleDeadLetterMessage(MessageDto messageDto, Map<String, Object> headers) {
        log.error("Handling dead letter message with ID: {} from Kafka topic: {}, partition: {}, offset: {}", 
                 messageDto.getId(), messageDto.getTopic(), messageDto.getPartition(), messageDto.getOffset());
        deadLetterCount.incrementAndGet();
        
        // Here you could implement logic to:
        // 1. Save to database for manual review
        // 2. Send alert notifications
        // 3. Log to external monitoring system
        // 4. Attempt alternative processing
        
        log.error("Dead letter message details - ID: {}, Type: {}, Content: {}, Retry Count: {}, Kafka Metadata: [Topic: {}, Partition: {}, Offset: {}]", 
                 messageDto.getId(), messageDto.getType(), messageDto.getContent(), messageDto.getRetryCount(),
                 messageDto.getTopic(), messageDto.getPartition(), messageDto.getOffset());
    }

    private void simulateProcessingDelay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Processing interrupted", e);
        }
    }

    // Metrics methods
    public long getProcessedCount() {
        return processedCount.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }

    public long getDeadLetterCount() {
        return deadLetterCount.get();
    }

    public Map<String, MessageDto> getProcessedMessages() {
        return new ConcurrentHashMap<>(processedMessages);
    }

    public void clearProcessedMessages() {
        processedMessages.clear();
        processedCount.set(0);
        failedCount.set(0);
        deadLetterCount.set(0);
        log.info("Cleared all processed message records");
    }
}