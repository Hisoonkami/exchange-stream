package com.adev.exchange.stream.exchange.coinone;

import com.adev.common.exchange.ProductSubscription;
import com.adev.common.exchange.StreamingExchange;
import com.adev.common.exchange.StreamingMarketDataService;
import com.adev.common.exchange.http.service.HttpStreamingService;
import io.reactivex.Completable;

public class CoinoneStreamingExchange implements StreamingExchange {

    private final HttpStreamingService httpStreamingService;
    private CoinoneStreamingMarketDataService streamingMarketDataService;

    public CoinoneStreamingExchange() {
        httpStreamingService =new HttpStreamingService("coinOne");
        streamingMarketDataService=new CoinoneStreamingMarketDataService(httpStreamingService);
    }

    @Override
    public Completable connect(ProductSubscription... args) {
        return httpStreamingService.connect();
    }

    @Override
    public Completable disconnect() {
        return httpStreamingService.disconnect();
    }

    @Override
    public boolean isAlive() {
        return httpStreamingService.isAlive();
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketDataService;
    }
}
