package com.learn.streams.orders.management.service;

import static com.learn.streams.orders.management.topology.OrdersTopology.*;
import static com.learn.streams.orders.management.util.OrderTypeMapper.getOrderType;
import static java.util.Objects.nonNull;

import com.learn.streams.orders.domain.OrderType;
import com.learn.streams.orders.domain.TotalRevenue;
import com.learn.streams.orders.dto.HostInfoDTO;
import com.learn.streams.orders.management.client.OrderServiceClient;
import com.learn.streams.orders.management.dto.AllOrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.OrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.OrdersRevenueDTO;
import com.learn.streams.orders.management.exception.api.OrdersStreamsException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

  private final OrderStoreService storeService;
  private final MetaDataService metaDataService;
  private final OrderServiceClient orderServiceClient;

  @Value("${server.port}")
  private Integer port;

  @Autowired
  public OrderService(
      OrderStoreService storeService,
      MetaDataService metaDataService,
      OrderServiceClient orderServiceClient) {
    this.storeService = storeService;
    this.metaDataService = metaDataService;
    this.orderServiceClient = orderServiceClient;
  }

  public Map<String, List<AllOrdersPerStoreDTO>> getAllOrdersCount(boolean queryOtherHosts) {
    final ReadOnlyKeyValueStore<String, Long> generalOrdersCountStore =
        getOrdersCountStore(OrderType.GENERAL);
    final ReadOnlyKeyValueStore<String, Long> restaurantOrdersCountStore =
        getOrdersCountStore(OrderType.RESTAURANT);

    final KeyValueIterator<String, Long> generalOrders = generalOrdersCountStore.all();
    final KeyValueIterator<String, Long> restaurantOrders = restaurantOrdersCountStore.all();

    final List<AllOrdersPerStoreDTO> orders = new ArrayList<>();

    if (queryOtherHosts) {
      orders.addAll(
          getOtherHosts().stream()
              .map(orderServiceClient::retrieveAllOrderCount)
              .flatMap(stringListMap -> stringListMap.values().stream())
              .flatMap(Collection::stream)
              .toList());
    }

    StreamSupport.stream(Spliterators.spliteratorUnknownSize(generalOrders, 0), false)
        .forEach(
            keyvalue -> {
              final AllOrdersPerStoreDTO ordersPerStoreDTO =
                  new AllOrdersPerStoreDTO(keyvalue.key, keyvalue.value, OrderType.GENERAL);
              orders.add(ordersPerStoreDTO);
            });
    StreamSupport.stream(Spliterators.spliteratorUnknownSize(restaurantOrders, 0), false)
        .forEach(
            keyvalue -> {
              final AllOrdersPerStoreDTO ordersPerStoreDTO =
                  new AllOrdersPerStoreDTO(keyvalue.key, keyvalue.value, OrderType.RESTAURANT);
              orders.add(ordersPerStoreDTO);
            });
    return orders.stream().collect(Collectors.groupingBy(AllOrdersPerStoreDTO::storeId));
  }

  public List<OrdersPerStoreDTO> getOrderCount(String type, boolean queryOtherHosts) {
    final OrderType orderType = getOrderType(type);

    final ReadOnlyKeyValueStore<String, Long> ordersCountStore = getOrdersCountStore(orderType);
    final KeyValueIterator<String, Long> orders = ordersCountStore.all();
    final Spliterator<KeyValue<String, Long>> spliterator =
        Spliterators.spliteratorUnknownSize(orders, 0);

    List<OrdersPerStoreDTO> otherOrders =
        retrieveOrdersFromOtherInstances(orderType, queryOtherHosts);

    // Aggregate the data
    return Stream.of(
            StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersPerStoreDTO(keyValue.key, keyValue.value))
                .toList(),
            otherOrders)
        .flatMap(Collection::stream)
        .toList();
  }

  public OrdersPerStoreDTO getOrderCountPerStore(String type, String locationId) {
    final OrderType orderType = getOrderType(type);

    HostInfoDTO hostInfoDTO =
        metaDataService.getStreamsMetaData(getOrdersCountStoreName(orderType), locationId);

    if (nonNull(hostInfoDTO)) {
      if (hostInfoDTO.port() != port) {
        // Retrieve from other instance
        return orderServiceClient.retrieveOrderCount(hostInfoDTO, orderType, locationId);
      }

      final ReadOnlyKeyValueStore<String, Long> ordersCountStore = getOrdersCountStore(orderType);
      final Long count = ordersCountStore.get(locationId);

      if (nonNull(count)) {
        return new OrdersPerStoreDTO(locationId, count);
      }
    }
    throw new OrdersStreamsException(
        String.format("Store %s not found", locationId), HttpStatus.NOT_FOUND);
  }

  public Map<String, List<OrdersRevenueDTO>> getTotalRevenue(boolean queryOtherHosts) {
    final ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenueStore =
        getRevenueStore(OrderType.GENERAL);
    final ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersRevenueStore =
        getRevenueStore(OrderType.RESTAURANT);

    final KeyValueIterator<String, TotalRevenue> generalOrdersRevenue =
        generalOrdersRevenueStore.all();
    final KeyValueIterator<String, TotalRevenue> restaurantOrdersRevenue =
        restaurantOrdersRevenueStore.all();

    final List<OrdersRevenueDTO> revenue = new ArrayList<>();

    if (queryOtherHosts) {
      revenue.addAll(
          getOtherHosts().stream()
              .map(orderServiceClient::retrieveTotalRevenue)
              .flatMap(map -> map.values().stream())
              .flatMap(Collection::stream)
              .toList());
    }

    StreamSupport.stream(Spliterators.spliteratorUnknownSize(generalOrdersRevenue, 0), false)
        .forEach(
            keyvalue -> {
              final OrdersRevenueDTO revenueDTO =
                  new OrdersRevenueDTO(keyvalue.key, OrderType.GENERAL, keyvalue.value);
              revenue.add(revenueDTO);
            });
    StreamSupport.stream(Spliterators.spliteratorUnknownSize(restaurantOrdersRevenue, 0), false)
        .forEach(
            keyvalue -> {
              final OrdersRevenueDTO revenueDTO =
                  new OrdersRevenueDTO(keyvalue.key, OrderType.RESTAURANT, keyvalue.value);
              revenue.add(revenueDTO);
            });
    return revenue.stream().collect(Collectors.groupingBy(OrdersRevenueDTO::storeId));
  }

  public List<OrdersRevenueDTO> getRevenue(String type, boolean queryOtherHosts) {
    final OrderType orderType = getOrderType(type);

    final ReadOnlyKeyValueStore<String, TotalRevenue> revenueStore = getRevenueStore(orderType);
    final KeyValueIterator<String, TotalRevenue> revenueIterator = revenueStore.all();

    List<OrdersRevenueDTO> otherOrdersRevenue =
        retrieveRevenueFromOtherInstances(orderType, queryOtherHosts);

    return Stream.of(
            StreamSupport.stream(Spliterators.spliteratorUnknownSize(revenueIterator, 0), false)
                .map(revenue -> new OrdersRevenueDTO(revenue.key, orderType, revenue.value))
                .toList(),
            otherOrdersRevenue)
        .flatMap(Collection::stream)
        .toList();
  }

  public OrdersRevenueDTO getRevenuePerStore(String type, String locationId) {
    final OrderType orderType = getOrderType(type);

    HostInfoDTO hostInfoDTO =
        metaDataService.getStreamsMetaData(getOrdersRevenueStoreName(orderType), locationId);

    if (nonNull(hostInfoDTO)) {
      if (hostInfoDTO.port() != port) {
        return orderServiceClient.retrieveOrdersRevenue(hostInfoDTO, orderType, locationId);
      }

      final ReadOnlyKeyValueStore<String, TotalRevenue> revenueStore = getRevenueStore(orderType);
      final TotalRevenue totalRevenue = revenueStore.get(locationId);

      if (nonNull(totalRevenue)) {
        return new OrdersRevenueDTO(locationId, orderType, totalRevenue);
      }
    }
    throw new OrdersStreamsException(
        String.format("Store %s not found", locationId), HttpStatus.NOT_FOUND);
  }

  private List<OrdersPerStoreDTO> retrieveOrdersFromOtherInstances(
      OrderType orderType, boolean queryOtherHosts) {
    // Fetch the info about other instances
    List<HostInfoDTO> otherHosts = getOtherHosts();
    if (queryOtherHosts && !otherHosts.isEmpty()) {
      // Make REST call to get data from other instances.
      // Make sure other instance is not going to make any network calls to other instances
      return otherHosts.stream()
          .map(host -> orderServiceClient.retrieveOrderCount(host, orderType))
          .flatMap(Collection::stream)
          .toList();
    }
    return List.of();
  }

  private List<OrdersRevenueDTO> retrieveRevenueFromOtherInstances(
      OrderType orderType, boolean queryOtherHosts) {
    // Fetch the info about other instances
    List<HostInfoDTO> otherHosts = getOtherHosts();
    if (queryOtherHosts && !otherHosts.isEmpty()) {
      // Make REST call to get data from other instances.
      // Make sure other instance is not going to make any network calls to other instances
      return otherHosts.stream()
          .map(host -> orderServiceClient.retrieveOrdersRevenue(host, orderType))
          .flatMap(Collection::stream)
          .toList();
    }
    return List.of();
  }

  private List<HostInfoDTO> getOtherHosts() {
    return metaDataService.getStreamsMetaData().stream()
        .filter(host -> host.port() != port)
        .toList();
  }

  private ReadOnlyKeyValueStore<String, Long> getOrdersCountStore(OrderType orderType) {
    return storeService.getOrderCountStore(getOrdersCountStoreName(orderType));
  }

  private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(OrderType orderType) {
    return storeService.getRevenueStore(getOrdersRevenueStoreName(orderType));
  }

  private String getOrdersCountStoreName(OrderType orderType) {
    return switch (orderType) {
      case GENERAL -> GENERAL_ORDERS_COUNT;
      case RESTAURANT -> RESTAURANT_ORDERS_COUNT;
    };
  }

  private String getOrdersRevenueStoreName(OrderType orderType) {
    return switch (orderType) {
      case GENERAL -> GENERAL_ORDERS_REVENUE;
      case RESTAURANT -> RESTAURANT_ORDERS_REVENUE;
    };
  }
}
