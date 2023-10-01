package com.learn.streams.orders.domain;

import static java.util.Objects.isNull;

import java.util.Arrays;
import java.util.Locale;

public enum OrderType {
  GENERAL,
  RESTAURANT;

  public static OrderType getValueOf(String value) throws IllegalArgumentException {
    if (isNull(value) || value.isBlank()) {
      throw new IllegalArgumentException("Unknown ORDER TYPE: " + value);
    }
    return Arrays.stream(OrderType.values())
        .filter(orderType -> orderType.name().equals(value.trim().toUpperCase(Locale.ROOT)))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown ORDER TYPE: " + value));
  }
}
