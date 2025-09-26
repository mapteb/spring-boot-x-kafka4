package com.example.kafkaconsumer.controller;

import com.example.kafkaconsumer.dto.MessageDto;
import com.example.kafkaconsumer.service.MessageProcessingService;
import com.example.kafkaconsumer.service.MessagePublisherService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(MessageController.class)
class MessageControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private MessageProcessingService messageProcessingService;

    @MockitoBean
    private MessagePublisherService messagePublisherService;

    private MessageDto validMessageDto;
    private String validMessageJson;

    @BeforeEach
    void setUp() throws Exception {
        validMessageDto = new MessageDto();
        validMessageDto.setContent("Test message content");
        validMessageDto.setType("order");

        validMessageJson = objectMapper.writeValueAsString(validMessageDto);

        // Setup default mock behavior
        when(messagePublisherService.getTopicName()).thenReturn("test-topic");
        doNothing().when(messagePublisherService).publishMessage(any(MessageDto.class));
    }

    // Tests for POST /api/messages/publish endpoint

    @Test
    void testPublishMessage_Success() throws Exception {
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.messageId").exists())
                .andExpect(jsonPath("$.message").value("Message published successfully"))
                .andExpect(jsonPath("$.targetTopic").value("test-topic"));

        verify(messagePublisherService).publishMessage(any(MessageDto.class));
        verify(messagePublisherService).getTopicName();
    }

    @Test
    void testPublishMessage_WithExistingId() throws Exception {
        validMessageDto.setId("existing-id-123");
        validMessageDto.setTimestamp(LocalDateTime.now());
        String messageWithId = objectMapper.writeValueAsString(validMessageDto);

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(messageWithId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.messageId").value("existing-id-123"))
                .andExpect(jsonPath("$.message").value("Message published successfully"))
                .andExpect(jsonPath("$.targetTopic").value("test-topic"));

        verify(messagePublisherService).publishMessage(argThat(msg -> 
            "existing-id-123".equals(msg.getId()) && msg.getTimestamp() != null));
    }

    @Test
    void testPublishMessage_GeneratesIdWhenNull() throws Exception {
        // validMessageDto already has null id
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messageId").isString())
                .andExpect(jsonPath("$.messageId").isNotEmpty());

        verify(messagePublisherService).publishMessage(argThat(msg -> msg.getId() != null));
    }

    @Test
    void testPublishMessage_GeneratesTimestampWhenNull() throws Exception {
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isOk());

        verify(messagePublisherService).publishMessage(argThat(msg -> msg.getTimestamp() != null));
    }

    @Test
    void testPublishMessage_ValidationError_BlankContent() throws Exception {
        validMessageDto.setContent(""); // Blank content should fail validation
        String invalidJson = objectMapper.writeValueAsString(validMessageDto);

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
                .andExpect(status().isBadRequest());

        verify(messagePublisherService, never()).publishMessage(any(MessageDto.class));
    }

    @Test
    void testPublishMessage_ValidationError_BlankType() throws Exception {
        validMessageDto.setType(""); // Blank type should fail validation
        String invalidJson = objectMapper.writeValueAsString(validMessageDto);

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
                .andExpect(status().isBadRequest());

        verify(messagePublisherService, never()).publishMessage(any(MessageDto.class));
    }

    @Test
    void testPublishMessage_ValidationError_NullContent() throws Exception {
        validMessageDto.setContent(null);
        String invalidJson = objectMapper.writeValueAsString(validMessageDto);

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
                .andExpect(status().isBadRequest());

        verify(messagePublisherService, never()).publishMessage(any(MessageDto.class));
    }

    @Test
    void testPublishMessage_ValidationError_NullType() throws Exception {
        validMessageDto.setType(null);
        String invalidJson = objectMapper.writeValueAsString(validMessageDto);

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
                .andExpect(status().isBadRequest());

        verify(messagePublisherService, never()).publishMessage(any(MessageDto.class));
    }

    @Test
    void testPublishMessage_InvalidJson() throws Exception {
        String invalidJson = "{ invalid json }";

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidJson))
                .andExpect(status().isBadRequest());

        verify(messagePublisherService, never()).publishMessage(any(MessageDto.class));
    }

    @Test
    void testPublishMessage_EmptyBody() throws Exception {
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(""))
                .andExpect(status().isBadRequest());

        verify(messagePublisherService, never()).publishMessage(any(MessageDto.class));
    }

    //TODO:
    @Disabled
    @Test
    void testPublishMessage_ServiceException() throws Exception {
        doThrow(new RuntimeException("Kafka connection failed"))
                .when(messagePublisherService).publishMessage(any(MessageDto.class));

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isInternalServerError());

        verify(messagePublisherService).publishMessage(any(MessageDto.class));
    }

    @Test
    void testPublishMessage_WithCompleteMessageDto() throws Exception {
        MessageDto completeMessage = new MessageDto();
        completeMessage.setId("test-id-123");
        completeMessage.setContent("Complete message content");
        completeMessage.setType("payment");
        completeMessage.setTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0));
        completeMessage.setSource("test-source");
        completeMessage.setPriority("high");
        completeMessage.setRetryCount(0);

        String completeJson = objectMapper.writeValueAsString(completeMessage);

        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(completeJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.messageId").value("test-id-123"));

        verify(messagePublisherService).publishMessage(argThat(msg -> 
            "test-id-123".equals(msg.getId()) &&
            "Complete message content".equals(msg.getContent()) &&
            "payment".equals(msg.getType()) &&
            "test-source".equals(msg.getSource()) &&
            "high".equals(msg.getPriority())
        ));
    }

    // Tests for GET /api/messages/processed endpoint

    @Test
    void testGetProcessedMessages_EmptyMap() throws Exception {
        Map<String, MessageDto> emptyMap = new ConcurrentHashMap<>();
        when(messageProcessingService.getProcessedMessages()).thenReturn(emptyMap);

        mockMvc.perform(get("/api/messages/processed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isEmpty());

        verify(messageProcessingService).getProcessedMessages();
    }

    @Test
    void testGetProcessedMessages_WithMessages() throws Exception {
        Map<String, MessageDto> processedMessages = new ConcurrentHashMap<>();
        
        MessageDto message1 = new MessageDto("id1", "content1", "order");
        MessageDto message2 = new MessageDto("id2", "content2", "payment");
        
        processedMessages.put("id1", message1);
        processedMessages.put("id2", message2);

        when(messageProcessingService.getProcessedMessages()).thenReturn(processedMessages);

        mockMvc.perform(get("/api/messages/processed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id1").exists())
                .andExpect(jsonPath("$.id1.content").value("content1"))
                .andExpect(jsonPath("$.id1.type").value("order"))
                .andExpect(jsonPath("$.id2").exists())
                .andExpect(jsonPath("$.id2.content").value("content2"))
                .andExpect(jsonPath("$.id2.type").value("payment"));

        verify(messageProcessingService).getProcessedMessages();
    }

    //TODO:
    @Disabled
    @Test
    void testGetProcessedMessages_ServiceException() throws Exception {
        when(messageProcessingService.getProcessedMessages())
                .thenThrow(new RuntimeException("Database connection failed"));

        mockMvc.perform(get("/api/messages/processed"))
                .andExpect(status().isInternalServerError());

        verify(messageProcessingService).getProcessedMessages();
    }

    @Test
    void testGetProcessedMessages_WithComplexMessages() throws Exception {
        Map<String, MessageDto> processedMessages = new ConcurrentHashMap<>();
        
        MessageDto complexMessage = new MessageDto();
        complexMessage.setId("complex-id");
        complexMessage.setContent("Complex message content");
        complexMessage.setType("notification");
        complexMessage.setTimestamp(LocalDateTime.of(2024, 1, 1, 10, 30));
        complexMessage.setSource("test-service");
        complexMessage.setPriority("medium");
        complexMessage.setRetryCount(2);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 123);
        complexMessage.setMetadata(metadata);
        
        processedMessages.put("complex-id", complexMessage);

        when(messageProcessingService.getProcessedMessages()).thenReturn(processedMessages);

        mockMvc.perform(get("/api/messages/processed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.['complex-id']").exists())
                .andExpect(jsonPath("$.['complex-id'].content").value("Complex message content"))
                .andExpect(jsonPath("$.['complex-id'].type").value("notification"))
                .andExpect(jsonPath("$.['complex-id'].source").value("test-service"))
                .andExpect(jsonPath("$.['complex-id'].priority").value("medium"))
                .andExpect(jsonPath("$.['complex-id'].retryCount").value(2))
                .andExpect(jsonPath("$.['complex-id'].metadata.key1").value("value1"))
                .andExpect(jsonPath("$.['complex-id'].metadata.key2").value(123));

        verify(messageProcessingService).getProcessedMessages();
    }

    // Integration-style tests

    @Test
    void testPublishThenGetProcessed() throws Exception {
        // First publish a message
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isOk());

        // Setup processed messages for the GET call
        Map<String, MessageDto> processedMessages = new ConcurrentHashMap<>();
        MessageDto processedMessage = new MessageDto("test-id", "Test message content", "order");
        processedMessages.put("test-id", processedMessage);
        when(messageProcessingService.getProcessedMessages()).thenReturn(processedMessages);

        // Then get processed messages
        mockMvc.perform(get("/api/messages/processed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isNotEmpty());

        verify(messagePublisherService).publishMessage(any(MessageDto.class));
        verify(messageProcessingService).getProcessedMessages();
    }

    @Test
    void testEndpointAccessibility() throws Exception {
        // Test that endpoints are accessible with correct HTTP methods
        
        // POST should work
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isOk());

        // GET should work for processed messages
        when(messageProcessingService.getProcessedMessages()).thenReturn(new ConcurrentHashMap<>());
        mockMvc.perform(get("/api/messages/processed"))
                .andExpect(status().isOk());

        // Wrong HTTP methods should return 405 Method Not Allowed
        mockMvc.perform(get("/api/messages/publish"))
                .andExpect(status().isMethodNotAllowed());

        mockMvc.perform(post("/api/messages/processed"))
                .andExpect(status().isMethodNotAllowed());
    }

    @Test
    void testContentTypeHandling() throws Exception {
        // Test with wrong content type
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.TEXT_PLAIN)
                .content(validMessageJson))
                .andExpect(status().isUnsupportedMediaType());

        // Test with correct content type
        mockMvc.perform(post("/api/messages/publish")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validMessageJson))
                .andExpect(status().isOk());
    }
}