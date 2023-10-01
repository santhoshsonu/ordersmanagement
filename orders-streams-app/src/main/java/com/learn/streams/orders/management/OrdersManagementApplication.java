package com.learn.streams.orders.management;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class OrdersManagementApplication {
  public static void main(String[] args) {
    SpringApplication.run(OrdersManagementApplication.class, args);
  }
}
