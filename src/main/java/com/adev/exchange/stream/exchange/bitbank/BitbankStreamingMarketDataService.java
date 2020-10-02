package com.adev.exchange.stream.exchange.bitbank;

import com.adev.common.base.domian.*;
import com.adev.common.base.utils.DataUtils;
import com.adev.common.exchange.StreamingMarketDataService;
import com.adev.common.exchange.exception.NotYetImplementedForExchangeException;
import com.adev.common.exchange.utils.TradeTypeUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author xn 2018/10/27 9:17
 */
public class BitbankStreamingMarketDataService implements StreamingMarketDataService {

	private static final Logger LOG = LoggerFactory.getLogger(BitbankStreamingMarketDataService.class);

	private final BitbankStreamingService service;

	public BitbankStreamingMarketDataService(BitbankStreamingService service) {
		this.service = service;
	}

	/**
	 * {"op":"subscribepub","args":["ticker@BTC_USDT"]}
	 */
	@Override
	public Observable<Ticker> getTicker(String currencyPair, Object... args) {
		String channelNameTemplate = "ticker_%s";
		String pair = (currencyPair.replace("-","_")).toLowerCase();
		String channelName = String.format(channelNameTemplate, pair);
		Observable<JsonNode> subscribedChannel = service.subscribeChannel(channelName, args);

		return subscribedChannel.map(node -> {
			Ticker ticker=new Ticker();
			if(node.has("message")){
				node = node.get("message");
				if(node.has("data")){
					JsonNode tickerNode = node.get("data");
					ticker.setExchange("bitbank");
					ticker.setCurrencyPair(currencyPair);
					ticker.setLast(DataUtils.objToBigDecimal(tickerNode.get("last")));
					ticker.setHigh(DataUtils.objToBigDecimal(tickerNode.get("high")));
					ticker.setLow(DataUtils.objToBigDecimal(tickerNode.get("low")));
					ticker.setVolume(DataUtils.objToBigDecimal(tickerNode.get("vol")));
					ticker.setTimestamp(Long.parseLong(tickerNode.get("timestamp").asText()));
				}
			}
			return ticker;
		});
	}

	/**
	 * {"op":"subscribepub","args":["deal@BTC_USDT"]}')
	 * {"op":"subscribepub","args":["deal@FUTURE_BTCUSD"]}')
	 */
	@Override
	public Observable<Trade> getTrades(String currencyPair, Object... args) {

		String channel = String.format("transactions_%s", currencyPair.replace("-", "_").toLowerCase());
		
		Observable<JsonNode> subscribedChannel = service.subscribeChannel(channel, args);

		return subscribedChannel.flatMapIterable(node -> {
			List<Trade> trades = new ArrayList<>();
			if(node.has("message")) {
				node = node.get("message");
				if (node.has("data")) {
					JsonNode dataNode = node.get("data");
					if (dataNode != null) {
						ArrayNode transactionsNode=(ArrayNode)dataNode.get("transactions");
						if(null!=transactionsNode){
							for (JsonNode item:transactionsNode){
								Trade trade=new Trade();
								trade.setExchange("bitbank");
								trade.setCurrencyPair(currencyPair);
								trade.setPrice(DataUtils.objToBigDecimal(item.get("price")));
								trade.setOriginalAmount(DataUtils.objToBigDecimal(item.get("amount")));
								trade.setTradeType(TradeTypeUtil.getOrderType(item.get("side").asText().toLowerCase()));
								trade.setTimestamp(DataUtils.objToLong(item.get("executed_at")));
								trade.setTradeId(DataUtils.objToLong(item.get("transaction_id")));
								trades.add(trade);
							}
						}
					}
				}
			}
			return trades;
		});
	}

	/**
	 * {"op":"subscribepub","args":["orderbook@BTC_USDT@1@2@10"]}
	 * {"op":"subscribepub","args":["orderbook@FUTURE_BTCUSD@1@2@10"]}
	 * {"args":["orderbook@FUTURE_BTC_USD@0@1@20"],"op":"subscribepub"}
	 */
	@Override
	public Observable<OrderBook> getOrderBook(String currencyPair, Object... args) {
		String channel = String.format("depth_whole_%s", currencyPair.replace("-", "_").toLowerCase());

		Observable<JsonNode> subscribedChannel = service.subscribeChannel(channel, args);
		return subscribedChannel.map(node -> {
			OrderBook orderBook=new OrderBook();
			orderBook.setExchange("bitbank");
			orderBook.setCurrencyPair(currencyPair);
			if(node.has("message")) {
				node = node.get("message");
				if(node.has("data")) {
					JsonNode depthNode = node.get("data");
					ArrayNode bidsNode=(ArrayNode)depthNode.get("bids");
					ArrayNode asksNode=(ArrayNode)depthNode.get("asks");
					Long timestamp=depthNode.get("timestamp").asLong();
					orderBook.setTimestamp(timestamp);
					if(asksNode != null && bidsNode != null){
						List<PriceAndVolume> bids=new ArrayList<>();
						Iterator<JsonNode> bidIte = bidsNode.iterator();
						while(bidIte.hasNext()){
							ArrayNode itemArrayNode=(ArrayNode)bidIte.next();
							PriceAndVolume priceAndVolume=new PriceAndVolume(DataUtils.objToBigDecimal(itemArrayNode.get(0)),DataUtils.objToBigDecimal(itemArrayNode.get(1)));
							bids.add(priceAndVolume);
						}
						orderBook.setBids(bids);
						Iterator<JsonNode> askIte = asksNode.iterator();
						List<PriceAndVolume> asks=new ArrayList<>();
						while(askIte.hasNext()){
							ArrayNode itemArrayNode=(ArrayNode)askIte.next();
							PriceAndVolume priceAndVolume=new PriceAndVolume(DataUtils.objToBigDecimal(itemArrayNode.get(0)),DataUtils.objToBigDecimal(itemArrayNode.get(1)));
							asks.add(priceAndVolume);
						}
						orderBook.setAsks(asks);
					}
				}
			}
			return orderBook;
		});
	}

	@Override
	public Observable<Kline> getCurrentKLine(String currencyPair, Object... args) {
		throw new NotYetImplementedForExchangeException();
	}

	@Override
	public Observable<Kline> getHistoryKLine(String currencyPair, Object... args) {
		throw new NotYetImplementedForExchangeException();
	}

}
