package com.learn.streams.orders.domain;

import java.math.BigDecimal;

public record OrderLineItem(String item, Integer quantity, BigDecimal price) {}
