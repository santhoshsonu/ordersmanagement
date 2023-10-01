package com.learn.streams.orders.management.dto;

import com.learn.streams.orders.domain.OrderType;
import com.learn.streams.orders.domain.TotalRevenue;

public record OrdersRevenueDTO(String storeId, OrderType orderType, TotalRevenue totalRevenue) {}
