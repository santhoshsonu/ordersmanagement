package com.learn.streams.orders.management.controller;

import com.learn.streams.orders.management.dto.AllOrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.OrdersRevenueDTO;
import com.learn.streams.orders.management.dto.WindowedOrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.WindowedRevenuePerStoreDTO;
import com.learn.streams.orders.management.service.OrderService;
import com.learn.streams.orders.management.service.OrderWindowService;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

  private final OrderService orderService;
  private final OrderWindowService orderWindowService;

  @Autowired
  public OrderController(OrderService orderService, OrderWindowService orderWindowService) {
    this.orderService = orderService;
    this.orderWindowService = orderWindowService;
  }

  /**
   * Retrieves the count of all orders per store grouped by order type.
   *
   * @return Response entity containing a map with store IDs as keys and lists of
   *     AllOrdersPerStoreDTO as values
   */
  @GetMapping("/count")
  public ResponseEntity<Map<String, List<AllOrdersPerStoreDTO>>> getOrdersCount(
      @RequestParam(value = "query_other_hosts", required = false, defaultValue = "true")
          boolean queryOtherHosts) {
    return ResponseEntity.ok(orderService.getAllOrdersCount(queryOtherHosts));
  }

  @GetMapping("/count/{order_type}")
  public ResponseEntity<?> getOrdersCountByOrderType(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "store_id", required = false) String storeId,
      @RequestParam(value = "query_other_hosts", required = false, defaultValue = "true")
          boolean queryOtherHosts) {
    if (StringUtils.isNotBlank(storeId)) {
      return ResponseEntity.ok(orderService.getOrderCountPerStore(orderType, storeId));
    }
    return ResponseEntity.ok(orderService.getOrderCount(orderType, queryOtherHosts));
  }

  @GetMapping("/revenue")
  public ResponseEntity<Map<String, List<OrdersRevenueDTO>>> getOrdersRevenue(
      @RequestParam(value = "query_other_hosts", required = false, defaultValue = "true")
          boolean queryOtherHosts) {
    return ResponseEntity.ok(orderService.getTotalRevenue(queryOtherHosts));
  }

  @GetMapping("/revenue/{order_type}")
  public ResponseEntity<?> getOrdersRevenueByOrderType(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "store_id", required = false) String storeId,
      @RequestParam(value = "query_other_hosts", required = false, defaultValue = "true")
          boolean queryOtherHosts) {
    if (StringUtils.isNotBlank(storeId)) {
      return ResponseEntity.ok(orderService.getRevenuePerStore(orderType, storeId));
    }
    return ResponseEntity.ok(orderService.getRevenue(orderType, queryOtherHosts));
  }

  @GetMapping("/window/count/{order_type}")
  public ResponseEntity<?> getWindowedOrdersCountByOrderType(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "store_id", required = false) String storeId,
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "from_time", required = false)
          LocalDateTime fromTime,
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "to_time", required = false)
          LocalDateTime toTime) {
    if (StringUtils.isNotBlank(storeId)) {
      return ResponseEntity.ok(
          orderWindowService.getWindowedOrdersPerStore(orderType, storeId, fromTime, toTime));
    }
    return ResponseEntity.ok(
        orderWindowService.getWindowedOrdersCountByOrderType(orderType, fromTime, toTime));
  }

  @GetMapping("/window/count")
  public ResponseEntity<Map<String, List<WindowedOrdersPerStoreDTO>>> getWindowedOrdersCount(
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "from_time", required = false)
          LocalDateTime fromTime,
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "to_time", required = false)
          LocalDateTime toTime) {
    return ResponseEntity.ok(orderWindowService.getWindowedOrdersCount(fromTime, toTime));
  }

  @GetMapping("/window/revenue/{order_type}")
  public ResponseEntity<?> getWindowedOrdersRevenueByOrderType(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "store_id", required = false) String storeId,
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "from_time", required = false)
          LocalDateTime fromTime,
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "to_time", required = false)
          LocalDateTime toTime) {
    if (StringUtils.isNotBlank(storeId)) {
      return ResponseEntity.ok(
          orderWindowService.getWindowedRevenuePerStore(orderType, storeId, fromTime, toTime));
    }
    return ResponseEntity.ok(
        orderWindowService.getWindowedRevenueByOrderType(orderType, fromTime, toTime));
  }

  @GetMapping("/window/revenue")
  public ResponseEntity<Map<String, List<WindowedRevenuePerStoreDTO>>> getWindowedOrdersRevenue(
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "from_time", required = false)
          LocalDateTime fromTime,
      @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          @RequestParam(value = "to_time", required = false)
          LocalDateTime toTime) {
    return ResponseEntity.ok(orderWindowService.getWindowedOrdersRevenue(fromTime, toTime));
  }
}
