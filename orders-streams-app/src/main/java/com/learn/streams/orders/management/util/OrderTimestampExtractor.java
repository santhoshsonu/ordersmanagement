package com.learn.streams.orders.management.util;

import static java.util.Objects.nonNull;

import com.learn.streams.orders.domain.Order;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class OrderTimestampExtractor implements TimestampExtractor {
  @Override
  public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
    if (consumerRecord.value() instanceof Order order && nonNull(order.orderDateTime())) {
      LocalDateTime timestamp = order.orderDateTime();
      return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
    // TODO: ideally should throw error
    return partitionTime;
  }
}
