package com.learn.streams.orders.management.exception.handler;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class StreamsDeserializationExceptionHandler implements DeserializationExceptionHandler {

  private final AtomicInteger counter = new AtomicInteger(0);

  @Override
  public DeserializationHandlerResponse handle(
      ProcessorContext context,
      ConsumerRecord<byte[], byte[]> consumerRecord,
      Exception exception) {
    log.warn(
        "Error in deserialize : {}  record: {}",
        exception.getMessage(),
        new String(consumerRecord.value(), StandardCharsets.UTF_8));

    log.info("Counter: {}", counter.get());

    if (counter.get() < 2) {
      counter.getAndIncrement();
      return DeserializationHandlerResponse.CONTINUE;
    }
    return DeserializationHandlerResponse.FAIL;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // Configure this class with the given key-value pairs
  }
}
