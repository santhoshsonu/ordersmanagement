package com.learn.streams.orders.management.dto;

import com.learn.streams.orders.domain.OrderType;
import com.learn.streams.orders.domain.TotalRevenue;
import java.time.LocalDateTime;

public record WindowedRevenuePerStoreDTO(
    String storeId,
    OrderType orderType,
    TotalRevenue totalRevenue,
    LocalDateTime startTime,
    LocalDateTime endTime) {}
