package com.learn.streams.orders.management.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.streams.orders.domain.*;
import com.learn.streams.orders.management.util.OrderTimestampExtractor;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrdersTopology {
  public static final Duration WINDOW_SIZE = Duration.ofHours(1);
  public static final Duration GRACE_WINDOW_SIZE = Duration.ofMinutes(1);

  public static final String ORDERS_TOPIC = "orders";
  public static final String GENERAL_ORDERS_TOPIC = "general-orders";
  public static final String GENERAL_ORDERS_COUNT = "general-orders-count";
  public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general-orders-count-window";
  public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general-orders-revenue-window";
  public static final String GENERAL_ORDERS_REVENUE = "general-orders-revenue";
  public static final String RESTAURANT_ORDERS_TOPIC = "restaurant-orders";
  public static final String RESTAURANT_ORDERS_COUNT = "restaurant-orders-count";
  public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant-orders-count-window";
  public static final String RESTAURANT_ORDERS_REVENUE = "restaurant-orders-revenue";
  public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant-orders-revenue-window";
  public static final String STORES_TOPIC = "stores";

  private final Serde<String> stringSerde;
  private final JsonSerde<Store> storeJsonSerde;
  private final JsonSerde<Order> orderJsonSerde;
  private final JsonSerde<StoreOrderCount> storeOrderCountJsonSerde;
  private final JsonSerde<TotalRevenue> totalRevenueSerde;
  private final JsonSerde<TotalRevenueWithAddress> totalRevenueWithAddressJsonSerde;

  @Autowired
  public OrdersTopology(ObjectMapper objectMapper) {
    stringSerde = new Serdes.StringSerde();
    storeJsonSerde = new JsonSerde<>(Store.class, objectMapper);
    orderJsonSerde = new JsonSerde<>(Order.class, objectMapper);
    storeOrderCountJsonSerde = new JsonSerde<>(StoreOrderCount.class, objectMapper);
    totalRevenueSerde = new JsonSerde<>(TotalRevenue.class, objectMapper);
    totalRevenueWithAddressJsonSerde = new JsonSerde<>(TotalRevenueWithAddress.class, objectMapper);
  }

  @Autowired
  public void process(StreamsBuilder streamsBuilder) {

    KTable<String, Store> storeKTable =
        streamsBuilder.table(
            STORES_TOPIC,
            Consumed.with(stringSerde, storeJsonSerde),
            Materialized.as(STORES_TOPIC));

    KStream<String, Order> orderKStream =
        streamsBuilder.stream(
                ORDERS_TOPIC,
                Consumed.with(stringSerde, orderJsonSerde)
                    .withTimestampExtractor(new OrderTimestampExtractor()))
            .selectKey((key, value) -> value.locationId());

    Predicate<String, Order> generalOrderPredicate =
        (key, value) -> OrderType.GENERAL.equals(value.orderType());
    Predicate<String, Order> restaurantOrderPredicate =
        (key, value) -> OrderType.RESTAURANT.equals(value.orderType());

    orderKStream
        .split(Named.as("orderType-split"))
        .branch(
            generalOrderPredicate,
            Branched.withConsumer(
                generalOrdersStream -> {
                  generalOrdersStream.to(
                      GENERAL_ORDERS_TOPIC, Produced.with(Serdes.String(), orderJsonSerde));

                  aggregateOrdersCount(generalOrdersStream, storeKTable, GENERAL_ORDERS_COUNT);
                  aggregateOrderCountByTimeWindow(
                      generalOrdersStream, storeKTable, GENERAL_ORDERS_COUNT_WINDOWS);

                  aggregateOrdersRevenue(generalOrdersStream, storeKTable, GENERAL_ORDERS_REVENUE);
                  aggregateRevenueByTimeWindow(
                      generalOrdersStream, storeKTable, GENERAL_ORDERS_REVENUE_WINDOWS);
                }))
        .branch(
            restaurantOrderPredicate,
            Branched.withConsumer(
                restaurantOrdersStream -> {
                  restaurantOrdersStream.to(
                      RESTAURANT_ORDERS_TOPIC, Produced.with(Serdes.String(), orderJsonSerde));

                  aggregateOrdersCount(
                      restaurantOrdersStream, storeKTable, RESTAURANT_ORDERS_COUNT);
                  aggregateOrderCountByTimeWindow(
                      restaurantOrdersStream, storeKTable, RESTAURANT_ORDERS_COUNT_WINDOWS);

                  aggregateOrdersRevenue(
                      restaurantOrdersStream, storeKTable, RESTAURANT_ORDERS_REVENUE);
                  aggregateRevenueByTimeWindow(
                      restaurantOrdersStream, storeKTable, RESTAURANT_ORDERS_REVENUE_WINDOWS);
                }));
  }

  /**
   * Aggregates the count of orders per store.
   *
   * @param orderKStream the input stream of orders
   * @param storeKTable the table of stores
   * @param storeName the name of the store
   */
  private void aggregateOrdersCount(
      KStream<String, Order> orderKStream, KTable<String, Store> storeKTable, String storeName) {

    KTable<String, Long> countOrdersPerStoreId =
        orderKStream
            .groupByKey(Grouped.with(stringSerde, orderJsonSerde))
            .count(Named.as(storeName), Materialized.as(storeName));

    countOrdersPerStoreId.toStream().print(Printed.<String, Long>toSysOut().withLabel(storeName));

    String storeOrderCountTableName = MessageFormat.format("{0}-{1}", storeName, "by-store");
    ValueJoiner<Long, Store, StoreOrderCount> valueJoiner = StoreOrderCount::new;

    KTable<String, StoreOrderCount> storeOrderCountKTable =
        countOrdersPerStoreId.join(
            storeKTable,
            valueJoiner,
            Materialized.<String, StoreOrderCount, KeyValueStore<Bytes, byte[]>>as(
                    storeOrderCountTableName)
                .withKeySerde(stringSerde)
                .withValueSerde(storeOrderCountJsonSerde));

    storeOrderCountKTable
        .toStream()
        .print(Printed.<String, StoreOrderCount>toSysOut().withLabel(storeOrderCountTableName));
  }

  /**
   * Aggregates the order count by time window.
   *
   * @param orderKStream the order stream
   * @param storeKTable the store table
   * @param storeName the name of the store
   */
  private void aggregateOrderCountByTimeWindow(
      KStream<String, Order> orderKStream, KTable<String, Store> storeKTable, String storeName) {
    TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_WINDOW_SIZE);

    KTable<Windowed<String>, Long> countOrdersPerStoreIdInTimeWindow =
        orderKStream
            .map((key, value) -> KeyValue.pair(value.locationId(), value)) // Re-keying locationId
            .groupByKey(Grouped.with(stringSerde, orderJsonSerde))
            .windowedBy(timeWindows)
            .count(Named.as(storeName), Materialized.as(storeName))
            // Suppress the result and emit the data at every WINDOW_SIZE duration
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

    ValueJoiner<Long, Store, StoreOrderCount> valueJoiner = StoreOrderCount::new;
    Joined<String, Long, Store> joinParams =
        Joined.with(stringSerde, Serdes.Long(), storeJsonSerde);

    String ordersCountPerStore = MessageFormat.format("{0}-{1}", storeName, "by-store");

    countOrdersPerStoreIdInTimeWindow
        .toStream()
        .peek(
            (key, value) ->
                log.info(
                    "StoreName: {} StartTime: {} EndTime: {} Value: {}",
                    storeName,
                    LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault()),
                    LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault()),
                    value))
        .map((key, value) -> KeyValue.pair(key.key(), value))
        .join(storeKTable, valueJoiner, joinParams)
        .print(Printed.<String, StoreOrderCount>toSysOut().withLabel(ordersCountPerStore));
  }

  /**
   * Aggregates the revenue of orders for a specific store.
   *
   * @param orderKStream the stream of orders
   * @param storeKTable the table of stores
   * @param storeName the name of the store
   */
  private void aggregateOrdersRevenue(
      KStream<String, Order> orderKStream, KTable<String, Store> storeKTable, String storeName) {

    Initializer<TotalRevenue> revenueInitializer = TotalRevenue::new;
    Aggregator<String, Order, TotalRevenue> revenueAggregator =
        (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

    KTable<String, TotalRevenue> revenueKTable =
        orderKStream
            .groupByKey(Grouped.with(stringSerde, orderJsonSerde))
            .aggregate(
                revenueInitializer,
                revenueAggregator,
                Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(stringSerde)
                    .withValueSerde(totalRevenueSerde));

    String orderRevenuePerStore = MessageFormat.format("{0}-{1}", storeName, "by-store");

    ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner =
        TotalRevenueWithAddress::new;

    KTable<String, TotalRevenueWithAddress> revenuePerStoreKTable =
        revenueKTable.join(
            storeKTable,
            valueJoiner,
            Materialized.<String, TotalRevenueWithAddress, KeyValueStore<Bytes, byte[]>>as(
                    orderRevenuePerStore)
                .withKeySerde(stringSerde)
                .withValueSerde(totalRevenueWithAddressJsonSerde));

    revenuePerStoreKTable
        .toStream()
        .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(orderRevenuePerStore));
  }

  /**
   * Aggregates revenue by time window.
   *
   * @param orderKStream the stream of order key-value pairs
   * @param storeKTable the KTable of store key-value pairs
   * @param storeName the name of the store
   */
  private void aggregateRevenueByTimeWindow(
      KStream<String, Order> orderKStream, KTable<String, Store> storeKTable, String storeName) {
    TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_WINDOW_SIZE);
    Initializer<TotalRevenue> revenueInitializer = TotalRevenue::new;
    Aggregator<String, Order, TotalRevenue> revenueAggregator =
        (key, order, totalRevenue) -> totalRevenue.updateRunningRevenue(key, order);

    KTable<Windowed<String>, TotalRevenue> revenuePerStoreIdInTimeWindow =
        orderKStream
            .map((key, value) -> KeyValue.pair(value.locationId(), value)) // Re-keying locationId
            .groupByKey(Grouped.with(stringSerde, orderJsonSerde))
            .windowedBy(timeWindows)
            .aggregate(
                revenueInitializer,
                revenueAggregator,
                Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(stringSerde)
                    .withValueSerde(totalRevenueSerde))
            // Suppress the result and emit the data at every WINDOW_SIZE duration
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

    ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner =
        TotalRevenueWithAddress::new;
    Joined<String, TotalRevenue, Store> joinParams =
        Joined.with(stringSerde, totalRevenueSerde, storeJsonSerde);

    String revenuePerStore = MessageFormat.format("{0}-{1}", storeName, "by-store");

    revenuePerStoreIdInTimeWindow
        .toStream()
        .peek(
            (key, value) ->
                log.info(
                    "StoreName: {} StartTime: {} EndTime: {} Value: {}",
                    storeName,
                    LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault()),
                    LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault()),
                    value))
        .map((key, value) -> KeyValue.pair(key.key(), value))
        .join(storeKTable, valueJoiner, joinParams)
        .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(revenuePerStore));
  }
}
