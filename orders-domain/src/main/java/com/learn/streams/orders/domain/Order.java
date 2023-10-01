package com.learn.streams.orders.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record Order(
    Integer orderId,
    String locationId,
    BigDecimal finalPrice,
    OrderType orderType,
    List<OrderLineItem> orderLineItems,
    LocalDateTime orderDateTime) {}
