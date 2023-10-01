package com.learn.streams.orders.management.producer;

import static com.learn.streams.orders.management.producer.ProducerUtil.publishMessageSync;
import static java.lang.Thread.sleep;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.javafaker.Faker;
import com.learn.streams.orders.domain.*;
import com.learn.streams.orders.management.topology.OrdersTopology;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderMockDataProducer {

  private static final List<Store> stores =
      List.of(
          new Store(
              "CA_1",
              new Address("4759 William Haven", "West Corey", "West Donald", "CA", 90152),
              "(460)648-7647"),
          new Store(
              "IN_1",
              new Address(
                  "The Salt House", "Shakespeare Sarani Rd, Elgin", "Kolkata", "IN", 700017),
              "(319)748-9241"),
          new Store(
              "CA_2",
              new Address("242 Christine Glen", "Jonathan Pass", "Howardborough", "CA", 50995),
              "560-597-5351"),
          new Store(
              "EU_1",
              new Address("778 Brown Plaza", "Faulkner Knolls", "Hullport", "EU", 55626),
              "575-482-5938"),
          new Store(
              "LA_1",
              new Address("60975 Jessica Squares", "William Haven", "Mira Mesa", "LA", 34437),
              "(416)500-1499"));

  private static final List<String> storeIds = stores.stream().map(Store::locationId).toList();

  private static final Faker faker = new Faker();
  private static final Random random = new Random();
  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

  public static void main(String[] args) throws InterruptedException {
    stores();
    generalOrders();
    restaurantOrders();

    sendBulkOrders();
  }

  private static int generateRandomNumber(int maxBound) {
    return random.nextInt(maxBound) + 1;
  }

  private static void stores() {
    stores.forEach(OrderMockDataProducer::sendStore);
  }

  private static void sendBulkOrders() throws InterruptedException {
    int count = 0;
    while (count < 100) {
      List<Order> generateOrders = generateOrders(OrderType.GENERAL, generateRandomNumber(10));
      List<Order> restaurantOrders = generateOrders(OrderType.RESTAURANT, generateRandomNumber(10));
      Stream.of(generateOrders, restaurantOrders)
          .flatMap(Collection::stream)
          .forEach(OrderMockDataProducer::sendOrder);
      sleep(100);
      count++;
    }
  }

  private static void generalOrders() {
    final int numOfOrders = generateRandomNumber(10);
    generateOrders(OrderType.GENERAL, numOfOrders).forEach(OrderMockDataProducer::sendOrder);
  }

  private static void restaurantOrders() {
    final int numOfOrders = generateRandomNumber(10);
    generateOrders(OrderType.RESTAURANT, numOfOrders).forEach(OrderMockDataProducer::sendOrder);
  }

  private static List<Order> generateOrders(OrderType orderType, int numOfOrders) {
    List<Order> orders = new ArrayList<>(numOfOrders);

    for (int i = 0; i < numOfOrders; i++) {
      final int orderLineItemCount = generateRandomNumber(3);
      List<OrderLineItem> orderLineItems = new ArrayList<>(orderLineItemCount);
      BigDecimal finalPrice = BigDecimal.ZERO;

      for (int j = 0; j < orderLineItemCount; j++) {
        BigDecimal price = BigDecimal.valueOf(faker.number().randomDouble(2, 1, 100));
        orderLineItems.add(
            new OrderLineItem(
                faker.commerce().productName(), faker.number().randomDigitNotZero(), price));
        finalPrice = finalPrice.add(price);
      }

      orders.add(
          new Order(
              faker.number().randomDigitNotZero(),
              storeIds.get(generateRandomNumber(storeIds.size() - 1)),
              finalPrice,
              orderType,
              orderLineItems,
              LocalDateTime.now(ZoneOffset.UTC)));
    }
    return orders;
  }

  private static void sendOrder(Order order) {
    try {
      var orderJSON = objectMapper.writeValueAsString(order);
      var recordMetaData = publishMessageSync(OrdersTopology.ORDERS_TOPIC, null, orderJSON);
      log.info("Published the order message : {} ", recordMetaData);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static void sendStore(Store store) {
    try {
      var storeJson = objectMapper.writeValueAsString(store);
      var recordMetaData =
          publishMessageSync(OrdersTopology.STORES_TOPIC, store.locationId(), storeJson);
      log.info("Published the store message : {} ", recordMetaData);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
