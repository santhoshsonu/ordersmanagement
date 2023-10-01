package com.learn.streams.orders.management.exception.api;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ApiErrorResponse {
  private String message;
  private int statusCode;
}
