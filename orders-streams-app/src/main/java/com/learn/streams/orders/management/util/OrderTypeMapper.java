package com.learn.streams.orders.management.util;

import com.learn.streams.orders.domain.OrderType;
import com.learn.streams.orders.management.exception.api.OrdersStreamsException;
import org.springframework.http.HttpStatus;

public class OrderTypeMapper {

  private OrderTypeMapper() {}

  public static OrderType getOrderType(String type) {
    try {
      return OrderType.getValueOf(type);
    } catch (IllegalArgumentException e) {
      throw new OrdersStreamsException(e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }
}
