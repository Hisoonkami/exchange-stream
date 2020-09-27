package com.adev.exchange.stream.exchange.bitbank;

import com.adev.common.exchange.ProductSubscription;
import com.adev.common.exchange.StreamingExchange;
import com.adev.common.exchange.StreamingMarketDataService;
import io.reactivex.Completable;

/**
 * @author huangbaiping
 */
public class BitbankStreamingExchange implements StreamingExchange {

	public static String API_BASE_URI = "wss://stream.bitbank.cc/socket.io/?EIO=3&transport=websocket";

	private final BitbankStreamingService streamingService;
	private BitbankStreamingMarketDataService streamingMarketDataService;

	public BitbankStreamingExchange() {
		this.streamingService = new BitbankStreamingService(API_BASE_URI);
		initServices();
	}

	protected void initServices() {
		streamingMarketDataService = new BitbankStreamingMarketDataService(streamingService);
	}

	@Override
	public Completable connect(ProductSubscription... args) {
		return streamingService.connect(args);
	}

	@Override
	public Completable disconnect() {
		return streamingService.disconnect();
	}

	@Override
	public boolean isAlive() {
		return streamingService.isSocketOpen();
	}

	@Override
	public StreamingMarketDataService getStreamingMarketDataService() {
		return streamingMarketDataService;
	}

}
