package com.learn.streams.orders.management.exception.api;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class OrdersStreamsException extends RuntimeException {

  private HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;

  public OrdersStreamsException(String message) {
    super(message);
  }

  public OrdersStreamsException(String message, HttpStatus status) {
    super(message);
    this.status = status;
  }
}
