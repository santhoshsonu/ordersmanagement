package com.learn.streams.orders.management.config;

import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learn.streams.orders.management.exception.handler.StreamsProcessorCustomErrorHandler;
import com.learn.streams.orders.management.exception.handler.StreamsSerializationExceptionHandler;
import com.learn.streams.orders.management.topology.OrdersTopology;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

@Configuration
@Slf4j
public class OrdersStreamsConfiguration {

  private static final int PARTITIONS = 6;
  private static final short REPLICAS = 2;

  @Autowired KafkaProperties kafkaProperties;

  @Value("${server.port}")
  private Integer port;

  @Value("${spring.application.name}")
  private String applicationName;

  private ConsumerRecordRecoverer healer() {
    return (consumerRecord, e) ->
        log.warn(
            "Error in deserialize : {} record: {}",
            e.getMessage(),
            new String((byte[]) consumerRecord.value(), StandardCharsets.UTF_8));
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .registerModule(new Jdk8Module())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() throws UnknownHostException {
    Map<String, Object> streamsProperties = kafkaProperties.buildStreamsProperties();
    streamsProperties.put(
        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        RecoveringDeserializationExceptionHandler.class);
    streamsProperties.put(
        RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, healer());
    streamsProperties.put(
        StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        StreamsSerializationExceptionHandler.class);
    // Exactly once semantics, uses transactions
    streamsProperties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
    // Config for running multiple instances
    streamsProperties.put(
        StreamsConfig.APPLICATION_SERVER_CONFIG,
        String.format("%s:%s", InetAddress.getLocalHost().getHostAddress(), port));
    streamsProperties.put(
        StreamsConfig.STATE_DIR_CONFIG, String.format("%s%s", applicationName, port));

    return new KafkaStreamsConfiguration(streamsProperties);
  }

  @Bean
  public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
    return streamsBuilderFactoryBean ->
        streamsBuilderFactoryBean.setStreamsUncaughtExceptionHandler(
            new StreamsProcessorCustomErrorHandler());
  }

  @Bean
  public NewTopic ordersTopic() {
    return TopicBuilder.name(OrdersTopology.ORDERS_TOPIC)
        .partitions(PARTITIONS)
        .replicas(REPLICAS)
        .build();
  }

  @Bean
  public NewTopic storesTopic() {
    return TopicBuilder.name(OrdersTopology.STORES_TOPIC)
        .partitions(PARTITIONS)
        .replicas(REPLICAS)
        .build();
  }
}
