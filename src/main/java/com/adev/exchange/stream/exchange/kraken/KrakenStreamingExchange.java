package com.adev.exchange.stream.exchange.kraken;

import com.adev.common.exchange.ProductSubscription;
import com.adev.common.exchange.StreamingExchange;
import com.adev.common.exchange.StreamingMarketDataService;
import com.adev.common.exchange.http.service.HttpStreamingService;
import io.reactivex.Completable;

public class KrakenStreamingExchange implements StreamingExchange {

    private final HttpStreamingService httpStreamingService;
    private KrakenStreamingMarketDataService streamingMarketDataService;

    public KrakenStreamingExchange() {
        httpStreamingService =new HttpStreamingService("coinOne");
        streamingMarketDataService=new KrakenStreamingMarketDataService(httpStreamingService);
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
