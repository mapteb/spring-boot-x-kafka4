package com.example.kafkaconsumer.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.topic.name}")
    private String topicName;

    @Value("${app.kafka.topic.partitions}")
    private int partitions;

    @Value("${app.kafka.topic.replication-factor}")
    private int replicationFactor;

    @Value("${app.kafka.topic.retry-topic}")
    private String retryTopicName;

    @Value("${app.kafka.topic.dead-letter-topic}")
    private String deadLetterTopicName;

    // Kafka Admin Configuration (No Zookeeper needed with KRaft)
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // KRaft mode - no zookeeper connection needed
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configs.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
        return new KafkaAdmin(configs);
    }

    // Topic Creation (KRaft handles metadata, not Zookeeper)
    @Bean
    public NewTopic messageTopic() {
        return TopicBuilder.name(topicName)
                .partitions(partitions)
                .replicas(replicationFactor)
                .compact() // Use log compaction for better performance
                .build();
    }

    @Bean
    public NewTopic retryTopic() {
        return TopicBuilder.name(retryTopicName)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "86400000") // 1 day retention for retry topics
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetterTopicName)
                .partitions(partitions)
                .replicas(replicationFactor)
                .config("retention.ms", "604800000") // 7 days retention for DLT
                .build();
    }

    // Producer Configuration - Optimized for KRaft
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Reliability settings for KRaft
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        configProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Performance settings
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // Increased batch size
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Small delay for batching
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB buffer
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Fast compression
        
        // Timeout settings for KRaft
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());
        
        // Configure default topic
        template.setDefaultTopic(topicName);
        
        // Enable observations for metrics
        template.setObservationEnabled(true);
        
        return template;
    }

    // Consumer Configuration - Optimized for KRaft
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Deserializer configuration
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafkaconsumer.dto");
        configProps.put(JsonDeserializer.TYPE_MAPPINGS, "messageDto:com.example.kafkaconsumer.dto.MessageDto");
        
        // Performance settings for KRaft
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // 1KB minimum fetch
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50); // Increased for better throughput
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        
        // Session and heartbeat settings
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        // Commit settings
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // configProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Concurrency settings for better performance
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // Enable observations for metrics
        factory.getContainerProperties().setObservationEnabled(true);
            
        return factory;
    }
}