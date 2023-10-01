package com.learn.streams.orders.management.service;

import static java.util.Objects.nonNull;

import com.learn.streams.orders.domain.TotalRevenue;
import com.learn.streams.orders.management.exception.api.OrdersStreamsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class OrderStoreService {

  public static final String INSTANCE_NOT_STARTED_ERROR_MSG =
      "KafkaStreams Instance has not started yet";
  private final StreamsBuilderFactoryBean factoryBean;

  private final OrdersStreamsException serviceUnavailableException =
      new OrdersStreamsException(INSTANCE_NOT_STARTED_ERROR_MSG, HttpStatus.SERVICE_UNAVAILABLE);

  @Autowired
  public OrderStoreService(StreamsBuilderFactoryBean factoryBean) {
    this.factoryBean = factoryBean;
  }

  public ReadOnlyKeyValueStore<String, Long> getOrderCountStore(String storeName) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    if (nonNull(kafkaStreams)) {
      return kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }
    throw serviceUnavailableException;
  }

  public ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String storeName) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    if (nonNull(kafkaStreams)) {
      return kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }
    throw new OrdersStreamsException(INSTANCE_NOT_STARTED_ERROR_MSG);
  }

  public ReadOnlyWindowStore<String, Long> getWindowedOrderCountStore(String storeName) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    if (nonNull(kafkaStreams)) {
      return kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
    }
    throw serviceUnavailableException;
  }

  public ReadOnlyWindowStore<String, TotalRevenue> getWindowedOrderRevenueStore(String storeName) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    if (nonNull(kafkaStreams)) {
      return kafkaStreams.store(
          StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
    }
    throw serviceUnavailableException;
  }
}
