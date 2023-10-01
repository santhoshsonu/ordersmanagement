package com.learn.streams.orders.management.service;

import static com.learn.streams.orders.management.topology.OrdersTopology.*;
import static com.learn.streams.orders.management.util.OrderTypeMapper.getOrderType;
import static java.util.Objects.nonNull;

import com.learn.streams.orders.domain.OrderType;
import com.learn.streams.orders.domain.TotalRevenue;
import com.learn.streams.orders.management.dto.WindowedOrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.WindowedRevenuePerStoreDTO;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderWindowService {
  private final OrderStoreService orderStoreService;

  @Autowired
  public OrderWindowService(OrderStoreService orderStoreService) {
    this.orderStoreService = orderStoreService;
  }

  public List<WindowedOrdersPerStoreDTO> getWindowedOrdersPerStore(
      String type, String storeId, LocalDateTime fromTime, LocalDateTime toTime) {
    final OrderType orderType = getOrderType(type);

    final ReadOnlyWindowStore<String, Long> orderCountStore = getWindowedOrderCountStore(orderType);

    Instant timeFrom =
        nonNull(fromTime)
            ? fromTime.toInstant(ZoneOffset.UTC)
            : Instant.ofEpochMilli(0); // beginning of time = oldest available
    Instant timeTo =
        nonNull(toTime)
            ? toTime.toInstant(ZoneOffset.UTC)
            : Instant.now(); // now (in processing-time)

    final WindowStoreIterator<Long> ordersCountIterator =
        orderCountStore.fetch(storeId, timeFrom, timeTo);

    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(ordersCountIterator, 0), false)
        .map(
            orderCount ->
                new WindowedOrdersPerStoreDTO(
                    storeId,
                    orderCount.value,
                    orderType,
                    Instant.ofEpochMilli(orderCount.key).atZone(ZoneOffset.UTC).toLocalDateTime(),
                    Instant.ofEpochMilli(orderCount.key).atZone(ZoneOffset.UTC).toLocalDateTime()))
        .toList();
  }

  public List<WindowedOrdersPerStoreDTO> getWindowedOrdersCountByOrderType(
      String type, LocalDateTime fromTime, LocalDateTime toTime) {
    final OrderType orderType = getOrderType(type);

    final ReadOnlyWindowStore<String, Long> orderCountStore = getWindowedOrderCountStore(orderType);

    KeyValueIterator<Windowed<String>, Long> ordersCountIterator;
    if (nonNull(fromTime) && nonNull(toTime)) {
      ordersCountIterator =
          orderCountStore.fetchAll(
              fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    } else {
      ordersCountIterator = orderCountStore.all();
    }

    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(ordersCountIterator, 0), false)
        .map(orderCount -> mapToWindowedOrdersPerStoreDTO(orderType, orderCount))
        .toList();
  }

  public Map<String, List<WindowedOrdersPerStoreDTO>> getWindowedOrdersCount(
      LocalDateTime fromTime, LocalDateTime toTime) {
    final ReadOnlyWindowStore<String, Long> generalOrderCountStore =
        getWindowedOrderCountStore(OrderType.GENERAL);
    final ReadOnlyWindowStore<String, Long> restaurantOrderCountStore =
        getWindowedOrderCountStore(OrderType.RESTAURANT);

    KeyValueIterator<Windowed<String>, Long> generalOrdersIterator;
    KeyValueIterator<Windowed<String>, Long> restaurantOrdersIterator;

    if (nonNull(fromTime) && nonNull(toTime)) {
      generalOrdersIterator =
          generalOrderCountStore.fetchAll(
              fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
      restaurantOrdersIterator =
          restaurantOrderCountStore.fetchAll(
              fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    } else {
      generalOrdersIterator = generalOrderCountStore.all();
      restaurantOrdersIterator = restaurantOrderCountStore.all();
    }

    final List<WindowedOrdersPerStoreDTO> orders = new ArrayList<>();
    StreamSupport.stream(Spliterators.spliteratorUnknownSize(generalOrdersIterator, 0), false)
        .forEach(
            orderCount ->
                orders.add(mapToWindowedOrdersPerStoreDTO(OrderType.GENERAL, orderCount)));

    StreamSupport.stream(Spliterators.spliteratorUnknownSize(restaurantOrdersIterator, 0), false)
        .forEach(
            orderCount ->
                orders.add(mapToWindowedOrdersPerStoreDTO(OrderType.RESTAURANT, orderCount)));
    return orders.stream().collect(Collectors.groupingBy(WindowedOrdersPerStoreDTO::storeId));
  }

  public List<WindowedRevenuePerStoreDTO> getWindowedRevenuePerStore(
      String type, String storeId, LocalDateTime fromTime, LocalDateTime toTime) {
    final OrderType orderType = getOrderType(type);

    final ReadOnlyWindowStore<String, TotalRevenue> ordersRevenueStore =
        getWindowedOrderRevenueStore(orderType);

    Instant timeFrom =
        nonNull(fromTime)
            ? fromTime.toInstant(ZoneOffset.UTC)
            : Instant.ofEpochMilli(0); // beginning of time = oldest available
    Instant timeTo =
        nonNull(toTime)
            ? toTime.toInstant(ZoneOffset.UTC)
            : Instant.now(); // now (in processing-time)

    WindowStoreIterator<TotalRevenue> ordersRevenueIterator =
        ordersRevenueStore.fetch(storeId, timeFrom, timeTo);

    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(ordersRevenueIterator, 0), false)
        .map(
            orderRevenue ->
                new WindowedRevenuePerStoreDTO(
                    storeId,
                    orderType,
                    orderRevenue.value,
                    Instant.ofEpochMilli(orderRevenue.key).atZone(ZoneOffset.UTC).toLocalDateTime(),
                    Instant.ofEpochMilli(orderRevenue.key)
                        .atZone(ZoneOffset.UTC)
                        .toLocalDateTime()))
        .toList();
  }

  public List<WindowedRevenuePerStoreDTO> getWindowedRevenueByOrderType(
      String type, LocalDateTime fromTime, LocalDateTime toTime) {
    final OrderType orderType = getOrderType(type);

    final ReadOnlyWindowStore<String, TotalRevenue> ordersRevenueStore =
        getWindowedOrderRevenueStore(orderType);

    KeyValueIterator<Windowed<String>, TotalRevenue> ordersRevenueIterator;
    if (nonNull(fromTime) && nonNull(toTime)) {
      ordersRevenueIterator =
          ordersRevenueStore.fetchAll(
              fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    } else {
      ordersRevenueIterator = ordersRevenueStore.all();
    }

    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(ordersRevenueIterator, 0), false)
        .map(orderRevenue -> mapToWindowedRevenuePerStoreDTO(orderType, orderRevenue))
        .toList();
  }

  public Map<String, List<WindowedRevenuePerStoreDTO>> getWindowedOrdersRevenue(
      LocalDateTime fromTime, LocalDateTime toTime) {
    final ReadOnlyWindowStore<String, TotalRevenue> generalOrderRevenueStore =
        getWindowedOrderRevenueStore(OrderType.GENERAL);
    final ReadOnlyWindowStore<String, TotalRevenue> restaurantOrderRevenueStore =
        getWindowedOrderRevenueStore(OrderType.RESTAURANT);

    KeyValueIterator<Windowed<String>, TotalRevenue> generalOrdersIterator;
    KeyValueIterator<Windowed<String>, TotalRevenue> restaurantOrdersIterator;

    if (nonNull(fromTime) && nonNull(toTime)) {
      generalOrdersIterator =
          generalOrderRevenueStore.fetchAll(
              fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
      restaurantOrdersIterator =
          restaurantOrderRevenueStore.fetchAll(
              fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    } else {
      generalOrdersIterator = generalOrderRevenueStore.all();
      restaurantOrdersIterator = restaurantOrderRevenueStore.all();
    }

    final List<WindowedRevenuePerStoreDTO> orders = new ArrayList<>();
    StreamSupport.stream(Spliterators.spliteratorUnknownSize(generalOrdersIterator, 0), false)
        .forEach(
            orderRevenue ->
                orders.add(mapToWindowedRevenuePerStoreDTO(OrderType.GENERAL, orderRevenue)));

    StreamSupport.stream(Spliterators.spliteratorUnknownSize(restaurantOrdersIterator, 0), false)
        .forEach(
            orderRevenue ->
                orders.add(mapToWindowedRevenuePerStoreDTO(OrderType.RESTAURANT, orderRevenue)));
    return orders.stream().collect(Collectors.groupingBy(WindowedRevenuePerStoreDTO::storeId));
  }

  private ReadOnlyWindowStore<String, Long> getWindowedOrderCountStore(OrderType orderType) {
    return switch (orderType) {
      case GENERAL -> orderStoreService.getWindowedOrderCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
      case RESTAURANT -> orderStoreService.getWindowedOrderCountStore(
          RESTAURANT_ORDERS_COUNT_WINDOWS);
    };
  }

  private ReadOnlyWindowStore<String, TotalRevenue> getWindowedOrderRevenueStore(
      OrderType orderType) {
    return switch (orderType) {
      case GENERAL -> orderStoreService.getWindowedOrderRevenueStore(
          GENERAL_ORDERS_REVENUE_WINDOWS);
      case RESTAURANT -> orderStoreService.getWindowedOrderRevenueStore(
          RESTAURANT_ORDERS_REVENUE_WINDOWS);
    };
  }

  private WindowedOrdersPerStoreDTO mapToWindowedOrdersPerStoreDTO(
      OrderType orderType, KeyValue<Windowed<String>, Long> orderCount) {
    return new WindowedOrdersPerStoreDTO(
        orderCount.key.key(),
        orderCount.value,
        orderType,
        LocalDateTime.ofInstant(orderCount.key.window().startTime(), ZoneOffset.UTC),
        LocalDateTime.ofInstant(orderCount.key.window().endTime(), ZoneOffset.UTC));
  }

  private WindowedRevenuePerStoreDTO mapToWindowedRevenuePerStoreDTO(
      OrderType orderType, KeyValue<Windowed<String>, TotalRevenue> orderRevenue) {
    return new WindowedRevenuePerStoreDTO(
        orderRevenue.key.key(),
        orderType,
        orderRevenue.value,
        LocalDateTime.ofInstant(orderRevenue.key.window().startTime(), ZoneOffset.UTC),
        LocalDateTime.ofInstant(orderRevenue.key.window().endTime(), ZoneOffset.UTC));
  }
}
