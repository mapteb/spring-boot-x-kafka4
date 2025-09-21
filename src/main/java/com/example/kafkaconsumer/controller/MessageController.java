package com.example.kafkaconsumer.controller;

import com.example.kafkaconsumer.dto.MessageDto;
import com.example.kafkaconsumer.service.MessageProcessingService;
import com.example.kafkaconsumer.service.MessagePublisherService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProcessingService messageProcessingService;
    private final MessagePublisherService messagePublisherService;

    @PostMapping("/publish")
    public ResponseEntity<Map<String, String>> publishMessage(@Valid @RequestBody MessageDto messageDto) {
        if (messageDto.getId() == null) {
            messageDto.setId(UUID.randomUUID().toString());
        }
        if (messageDto.getTimestamp() == null) {
            messageDto.setTimestamp(LocalDateTime.now());
        }

        messagePublisherService.publishMessage(messageDto);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("messageId", messageDto.getId());
        response.put("message", "Message published successfully");
        response.put("targetTopic", messagePublisherService.getTopicName());
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/publish/topic/{topicName}")
    public ResponseEntity<Map<String, String>> publishToCustomTopic(
            @PathVariable String topicName,
            @Valid @RequestBody MessageDto messageDto) {
        
        if (messageDto.getId() == null) {
            messageDto.setId(UUID.randomUUID().toString());
        }
        if (messageDto.getTimestamp() == null) {
            messageDto.setTimestamp(LocalDateTime.now());
        }

        messagePublisherService.publishMessage(messageDto, topicName);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("messageId", messageDto.getId());
        response.put("message", "Message published to custom topic");
        response.put("targetTopic", topicName);
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/publish/partition/{partition}")
    public ResponseEntity<Map<String, String>> publishToPartition(
            @PathVariable int partition,
            @Valid @RequestBody MessageDto messageDto) {
        
        if (messageDto.getId() == null) {
            messageDto.setId(UUID.randomUUID().toString());
        }
        if (messageDto.getTimestamp() == null) {
            messageDto.setTimestamp(LocalDateTime.now());
        }

        messagePublisherService.publishWithPartition(messageDto, partition);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("messageId", messageDto.getId());
        response.put("message", "Message published to specific partition");
        response.put("targetTopic", messagePublisherService.getTopicName());
        response.put("partition", String.valueOf(partition));
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/publish/bulk")
    public ResponseEntity<Map<String, Object>> publishBulkMessages(
            @RequestParam(defaultValue = "10") int count,
            @RequestParam(defaultValue = "order") String type) {
        
        Map<String, Object> response = new HashMap<>();
        
        for (int i = 0; i < count; i++) {
            MessageDto message = new MessageDto(
                UUID.randomUUID().toString(),
                String.format("Test %s message #%d", type, i + 1),
                type
            );
            messagePublisherService.publishMessage(message);
        }
        
        response.put("status", "success");
        response.put("publishedCount", count);
        response.put("type", type);
        response.put("message", "Bulk messages published successfully");
        response.put("targetTopic", messagePublisherService.getTopicName());
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/publish/retry")
    public ResponseEntity<Map<String, String>> publishToRetryTopic(@Valid @RequestBody MessageDto messageDto) {
        if (messageDto.getId() == null) {
            messageDto.setId(UUID.randomUUID().toString());
        }
        if (messageDto.getTimestamp() == null) {
            messageDto.setTimestamp(LocalDateTime.now());
        }

        messagePublisherService.publishToRetryTopic(messageDto);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("messageId", messageDto.getId());
        response.put("message", "Message published to retry topic");
        // response.put("retryTopic", messagePublisherService.getRetryTopicName());
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/publish/dlt")
    public ResponseEntity<Map<String, String>> publishToDeadLetterTopic(@Valid @RequestBody MessageDto messageDto) {
        if (messageDto.getId() == null) {
            messageDto.setId(UUID.randomUUID().toString());
        }
        if (messageDto.getTimestamp() == null) {
            messageDto.setTimestamp(LocalDateTime.now());
        }

        messagePublisherService.publishToDeadLetterTopic(messageDto);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("messageId", messageDto.getId());
        response.put("message", "Message published to dead letter topic");
        // response.put("deadLetterTopic", messagePublisherService.getDeadLetterTopicName());
        
        return ResponseEntity.ok(response);
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getMessageStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("processedCount", messageProcessingService.getProcessedCount());
        stats.put("failedCount", messageProcessingService.getFailedCount());
        stats.put("deadLetterCount", messageProcessingService.getDeadLetterCount());
        stats.put("timestamp", LocalDateTime.now());
        
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/processed")
    public ResponseEntity<Map<String, MessageDto>> getProcessedMessages() {
        return ResponseEntity.ok(messageProcessingService.getProcessedMessages());
    }

    @DeleteMapping("/processed")
    public ResponseEntity<Map<String, String>> clearProcessedMessages() {
        messageProcessingService.clearProcessedMessages();
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Processed messages cleared");
        
        return ResponseEntity.ok(response);
    }

    @GetMapping("/topic-info")
    public ResponseEntity<Map<String, Object>> getTopicInfo() {
        Map<String, Object> topicInfo = new HashMap<>();
        topicInfo.put("mainTopic", messagePublisherService.getTopicName());
        // topicInfo.put("retryTopic", messagePublisherService.getRetryTopicName());
        // topicInfo.put("deadLetterTopic", messagePublisherService.getDeadLetterTopicName());
        topicInfo.put("kafkaMode", "KRaft (no Zookeeper)");
        topicInfo.put("explanation", "Messages are published to Kafka topics using KRaft consensus protocol");
        // topicInfo.put("kafkaHealthy", messagePublisherService.isKafkaHealthy());
        
        return ResponseEntity.ok(topicInfo);
    }

    @PostMapping("/test-error")
    public ResponseEntity<Map<String, String>> publishErrorMessage() {
        MessageDto errorMessage = new MessageDto(
            UUID.randomUUID().toString(),
            "This is an invalid message that should fail processing",
            "order"
        );
        
        messagePublisherService.publishMessage(errorMessage);
        
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("messageId", errorMessage.getId());
        response.put("message", "Error message published for testing");
        response.put("targetTopic", messagePublisherService.getTopicName());
        
        return ResponseEntity.ok(response);
    }
}

