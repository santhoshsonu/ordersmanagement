package com.learn.streams.orders.management.dto;

import com.learn.streams.orders.domain.OrderType;

public record AllOrdersPerStoreDTO(String storeId, Long numOfOrders, OrderType orderType) {}
