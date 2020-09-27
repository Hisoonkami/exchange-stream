package com.adev.exchange.stream.service;

import com.adev.exchange.stream.config.SubscribeConfig;

public interface SubscribeService {
    void subscribe(SubscribeConfig.SubscribeExchange exchange);
}
