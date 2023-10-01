package com.learn.streams.orders.management.exception.handler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

@Slf4j
public class StreamsSerializationExceptionHandler implements ProductionExceptionHandler {
  @Override
  public ProductionExceptionHandlerResponse handle(
      ProducerRecord<byte[], byte[]> producerRecord, Exception exception) {
    log.error(
        "Exception in serialize : {}  record: {}",
        exception.getMessage(),
        new String(producerRecord.value()));
    return ProductionExceptionHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(Map<String, ?> map) {
    // Configure this class with the given key-value pairs
  }
}
