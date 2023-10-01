package com.learn.streams.orders.domain;

public record Address(
    String addressLine1, String addressLine2, String city, String state, Integer zip) {}
