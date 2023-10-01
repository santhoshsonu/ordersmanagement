package com.learn.streams.orders.management.dto;

import com.learn.streams.orders.domain.OrderType;
import java.time.LocalDateTime;

public record WindowedOrdersPerStoreDTO(
    String storeId,
    Long numOfOrders,
    OrderType orderType,
    LocalDateTime startTime,
    LocalDateTime endTime) {}
