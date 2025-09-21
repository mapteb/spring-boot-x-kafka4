package com.example.kafkaconsumer.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageDto {

    @NotBlank(message = "Message ID cannot be blank")
    private String id = "1";

    @NotBlank(message = "Message content cannot be blank")
    private String content;

    @NotBlank(message = "Message type cannot be blank")
    private String type;

    @NotNull(message = "Timestamp cannot be null")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp = LocalDateTime.now();

    private String source;
    
    private String priority;
    
    private Map<String, Object> metadata;

    private Integer retryCount;

    // Kafka-specific fields
    private String topic;
    private Integer partition;
    private Long offset;

    public MessageDto(String id, String content, String type) {
        this.id = id;
        this.content = content;
        this.type = type;
        this.timestamp = LocalDateTime.now();
        this.retryCount = 0;
    }
}