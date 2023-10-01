package com.learn.streams.orders.management.exception.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamsProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {

  @Override
  public StreamThreadExceptionResponse handle(Throwable exception) {
    log.error("Exception in Application : {}", exception.getMessage(), exception);
    return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
  }
}
