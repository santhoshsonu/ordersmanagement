package com.learn.streams.orders.management.exception.handler;

import com.learn.streams.orders.management.exception.api.ApiErrorResponse;
import com.learn.streams.orders.management.exception.api.OrdersStreamsException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
@Slf4j
public class GlobalErrorHandler {

  @ExceptionHandler(OrdersStreamsException.class)
  public ResponseEntity<ApiErrorResponse> handleException(OrdersStreamsException e) {
    return new ResponseEntity<>(
        new ApiErrorResponse().setMessage(e.getMessage()).setStatusCode(e.getStatus().value()),
        e.getStatus());
  }

  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<ApiErrorResponse> handleException(RuntimeException e) {
    ApiErrorResponse errorResponse =
        new ApiErrorResponse()
            .setMessage("Oops! Something went wrong. Please try again later.")
            .setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
    if (e instanceof InvalidStateStoreException) {
      log.error("Unexpected error occurred: {}", e.getMessage());
      return new ResponseEntity<>(
          errorResponse
              .setMessage("Service not STARTED yet.")
              .setStatusCode(HttpStatus.SERVICE_UNAVAILABLE.value()),
          HttpStatus.SERVICE_UNAVAILABLE);
    }
    log.error("Unexpected error occurred", e);
    return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
