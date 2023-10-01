package com.learn.streams.orders.management.client;

import com.learn.streams.orders.domain.OrderType;
import com.learn.streams.orders.dto.HostInfoDTO;
import com.learn.streams.orders.management.dto.AllOrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.OrdersPerStoreDTO;
import com.learn.streams.orders.management.dto.OrdersRevenueDTO;
import com.learn.streams.orders.management.exception.api.ApiErrorResponse;
import com.learn.streams.orders.management.exception.api.OrdersStreamsException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class OrderServiceClient {
  private final WebClient webClient;
  private static final String BASE_URL_FORMAT = "http://%s:%s";

  @Autowired
  public OrderServiceClient(WebClient webClient) {
    this.webClient = webClient;
  }

  public Map<String, List<AllOrdersPerStoreDTO>> retrieveAllOrderCount(HostInfoDTO hostInfoDTO) {
    final String baseUrl = String.format(BASE_URL_FORMAT, hostInfoDTO.host(), hostInfoDTO.port());
    String url =
        UriComponentsBuilder.fromHttpUrl(baseUrl)
            .path("/api/v1/orders/count")
            .queryParam("query_other_hosts", "false")
            .build()
            .toString();
    log.info("Remote call to: {}", url);
    return webClient
        .get()
        .uri(url)
        .retrieve()
        .onStatus(HttpStatusCode::isError, this::handleError)
        .bodyToMono(new ParameterizedTypeReference<Map<String, List<AllOrdersPerStoreDTO>>>() {})
        .onErrorMap(
            Predicate.not(OrdersStreamsException.class::isInstance),
            ex -> new OrdersStreamsException(ex.getMessage()))
        .block();
  }

  public List<OrdersPerStoreDTO> retrieveOrderCount(HostInfoDTO hostInfoDTO, OrderType orderType) {
    final String baseUrl = String.format(BASE_URL_FORMAT, hostInfoDTO.host(), hostInfoDTO.port());
    String url =
        UriComponentsBuilder.fromHttpUrl(baseUrl)
            .path("/api/v1/orders/count/{order_type}")
            .queryParam("query_other_hosts", "false")
            .build(orderType)
            .toString();
    log.info("Remote call to: {}", url);
    return webClient
        .get()
        .uri(url)
        .retrieve()
        .onStatus(HttpStatusCode::isError, this::handleError)
        .bodyToFlux(OrdersPerStoreDTO.class)
        .onErrorMap(
            Predicate.not(OrdersStreamsException.class::isInstance),
            ex -> new OrdersStreamsException(ex.getMessage()))
        .collectList()
        .block();
  }

  public OrdersPerStoreDTO retrieveOrderCount(
      HostInfoDTO hostInfoDTO, OrderType orderType, String storeId) {
    final String baseUrl = String.format(BASE_URL_FORMAT, hostInfoDTO.host(), hostInfoDTO.port());
    String url =
        UriComponentsBuilder.fromHttpUrl(baseUrl)
            .path("/api/v1/orders/count/{order_type}")
            .queryParam("store_id", storeId)
            .queryParam("query_other_hosts", "false")
            .build(orderType)
            .toString();
    log.info("Remote call to: {}", url);
    return webClient
        .get()
        .uri(url)
        .retrieve()
        .onStatus(HttpStatusCode::isError, this::handleError)
        .bodyToMono(OrdersPerStoreDTO.class)
        .onErrorMap(
            Predicate.not(OrdersStreamsException.class::isInstance),
            ex -> new OrdersStreamsException(ex.getMessage()))
        .block();
  }

  public Map<String, List<OrdersRevenueDTO>> retrieveTotalRevenue(HostInfoDTO hostInfoDTO) {
    final String baseUrl = String.format(BASE_URL_FORMAT, hostInfoDTO.host(), hostInfoDTO.port());
    String url =
        UriComponentsBuilder.fromHttpUrl(baseUrl)
            .path("/api/v1/orders/revenue")
            .queryParam("query_other_hosts", "false")
            .build()
            .toString();
    log.info("Remote call to: {}", url);
    return webClient
        .get()
        .uri(url)
        .retrieve()
        .onStatus(HttpStatusCode::isError, this::handleError)
        .bodyToMono(new ParameterizedTypeReference<Map<String, List<OrdersRevenueDTO>>>() {})
        .onErrorMap(
            Predicate.not(OrdersStreamsException.class::isInstance),
            ex -> new OrdersStreamsException(ex.getMessage()))
        .block();
  }

  public List<OrdersRevenueDTO> retrieveOrdersRevenue(
      HostInfoDTO hostInfoDTO, OrderType orderType) {
    final String baseUrl = String.format(BASE_URL_FORMAT, hostInfoDTO.host(), hostInfoDTO.port());
    String url =
        UriComponentsBuilder.fromHttpUrl(baseUrl)
            .path("/api/v1/orders/revenue/{order_type}")
            .queryParam("query_other_hosts", "false")
            .build(orderType)
            .toString();
    log.info("Remote call to: {}", url);
    return webClient
        .get()
        .uri(url)
        .retrieve()
        .onStatus(HttpStatusCode::isError, this::handleError)
        .bodyToFlux(OrdersRevenueDTO.class)
        .onErrorMap(
            Predicate.not(OrdersStreamsException.class::isInstance),
            ex -> new OrdersStreamsException(ex.getMessage()))
        .collectList()
        .block();
  }

  public OrdersRevenueDTO retrieveOrdersRevenue(
      HostInfoDTO hostInfoDTO, OrderType orderType, String storeId) {
    final String baseUrl = String.format(BASE_URL_FORMAT, hostInfoDTO.host(), hostInfoDTO.port());
    String url =
        UriComponentsBuilder.fromHttpUrl(baseUrl)
            .path("/api/v1/orders/revenue/{order_type}")
            .queryParam("store_id", storeId)
            .queryParam("query_other_hosts", "false")
            .build(orderType)
            .toString();
    log.info("Remote call to: {}", url);
    return webClient
        .get()
        .uri(url)
        .retrieve()
        .onStatus(HttpStatusCode::isError, this::handleError)
        .bodyToMono(OrdersRevenueDTO.class)
        .onErrorMap(
            Predicate.not(OrdersStreamsException.class::isInstance),
            ex -> new OrdersStreamsException(ex.getMessage()))
        .block();
  }

  private Mono<OrdersStreamsException> handleError(ClientResponse response) {
    return response
        .bodyToMono(ApiErrorResponse.class)
        .flatMap(
            apiErrorResponse ->
                Mono.error(
                    new OrdersStreamsException(
                        apiErrorResponse.getMessage(),
                        HttpStatus.valueOf(apiErrorResponse.getStatusCode()))));
  }
}
