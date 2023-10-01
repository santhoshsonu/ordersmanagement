package com.learn.streams.orders.management.service;

import static com.learn.streams.orders.management.service.OrderStoreService.INSTANCE_NOT_STARTED_ERROR_MSG;
import static java.util.Objects.nonNull;

import com.learn.streams.orders.dto.HostInfoDTO;
import com.learn.streams.orders.management.exception.api.OrdersStreamsException;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class MetaDataService {

  private final StreamsBuilderFactoryBean factoryBean;

  @Autowired
  public MetaDataService(StreamsBuilderFactoryBean factoryBean) {
    this.factoryBean = factoryBean;
  }

  public List<HostInfoDTO> getStreamsMetaData() {
    final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    if (nonNull(kafkaStreams)) {
      return kafkaStreams.metadataForAllStreamsClients().stream()
          .map(metaData -> new HostInfoDTO(metaData.host(), metaData.port()))
          .toList();
    }
    throw new OrdersStreamsException(
        INSTANCE_NOT_STARTED_ERROR_MSG, HttpStatus.SERVICE_UNAVAILABLE);
  }

  public HostInfoDTO getStreamsMetaData(String storeName, String locationId) {
    final KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    if (nonNull(kafkaStreams)) {
      KeyQueryMetadata queryMetadata =
          kafkaStreams.queryMetadataForKey(storeName, locationId, Serdes.String().serializer());
      if (nonNull(queryMetadata)) {
        return new HostInfoDTO(
            queryMetadata.activeHost().host(), queryMetadata.activeHost().port());
      }
      return null;
    }
    throw new OrdersStreamsException(
        INSTANCE_NOT_STARTED_ERROR_MSG, HttpStatus.SERVICE_UNAVAILABLE);
  }
}
