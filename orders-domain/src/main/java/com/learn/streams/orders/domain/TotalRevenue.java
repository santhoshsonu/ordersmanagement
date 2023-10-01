package com.learn.streams.orders.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId, Long runningOrderCount, BigDecimal runningRevenue) {

  public TotalRevenue() {
    this("", 0L, BigDecimal.ZERO);
  }

  public TotalRevenue updateRunningRevenue(String key, Order order) {
    long newOrderCount = this.runningOrderCount + 1;
    return new TotalRevenue(key, newOrderCount, this.runningRevenue.add(order.finalPrice()));
  }
}
