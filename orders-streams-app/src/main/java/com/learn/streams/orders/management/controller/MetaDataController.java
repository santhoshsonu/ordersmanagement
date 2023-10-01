package com.learn.streams.orders.management.controller;

import com.learn.streams.orders.dto.HostInfoDTO;
import com.learn.streams.orders.management.service.MetaDataService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/orders/metadata")
public class MetaDataController {

  private final MetaDataService metaDataService;

  @Autowired
  public MetaDataController(MetaDataService metaDataService) {
    this.metaDataService = metaDataService;
  }

  @GetMapping("/all")
  public ResponseEntity<List<HostInfoDTO>> getMetaData() {
    return ResponseEntity.ok(metaDataService.getStreamsMetaData());
  }
}
