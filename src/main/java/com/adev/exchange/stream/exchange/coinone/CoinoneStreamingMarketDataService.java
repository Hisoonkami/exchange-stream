package com.adev.exchange.stream.exchange.coinone;

import com.adev.common.base.domian.*;
import com.adev.common.base.utils.DataUtils;
import com.adev.common.exchange.StreamingMarketDataService;
import com.adev.common.exchange.exception.NotYetImplementedForExchangeException;
import com.adev.common.exchange.http.domain.RequestParam;
import com.adev.common.exchange.http.service.HttpStreamingService;
import com.adev.common.exchange.utils.TradeTypeUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.reactivex.Observable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CoinoneStreamingMarketDataService implements StreamingMarketDataService {

    private final HttpStreamingService httpStreamingService;
    private ObjectMapper mapper = new ObjectMapper();

    public CoinoneStreamingMarketDataService(HttpStreamingService httpStreamingService) {
        this.httpStreamingService = httpStreamingService;
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Observable<Ticker> getTicker(String currencyPair, Object... args) {
        String url="https://api.coinone.co.kr/ticker/?currency=all";
        RequestParam requestParam=new RequestParam();
        requestParam.setUrl(url);
        return httpStreamingService.pollingRestApi(requestParam).flatMapIterable(s -> {
            List<Ticker> tickers = new ArrayList<>();
            JsonNode jsonNode = mapper.readTree(s);
            if (jsonNode.has("errorCode") && !jsonNode.get("errorCode").asText().equalsIgnoreCase("0")) {
                return tickers;
            }
            String timestamp = jsonNode.get("timestamp").asText();
            if(timestamp.length() == 10){
                timestamp = timestamp + "000";
            }
            Iterator<JsonNode> it = jsonNode.elements();
            while (it.hasNext()) {
                JsonNode tickerNode = it.next();
                if (tickerNode.size() > 1) {
                    String pair = tickerNode.get("currency").asText() + "-krw";
                    Ticker ticker=new Ticker();
                    ticker.setCurrencyPair(pair);
                    ticker.setExchange("coinone");
                    ticker.setLast(DataUtils.objToBigDecimal(tickerNode.get("last")));
                    ticker.setOpen(DataUtils.objToBigDecimal(tickerNode.get("first")));
                    ticker.setLow(DataUtils.objToBigDecimal(tickerNode.get("low")));
                    ticker.setHigh(DataUtils.objToBigDecimal(tickerNode.get("high")));
                    ticker.setVolume(DataUtils.objToBigDecimal(tickerNode.get("volume")));
                    ticker.setTimestamp(Long.parseLong(timestamp));
                    tickers.add(ticker);
                }

            }
            return tickers;
        });
    }

    @Override
    public Observable<Trade> getTrades(String currencyPair, Object... args) {
        String url="https://api.coinone.co.kr/trades/?currency="+currencyPair.split("-")[0]+"&period=hour";
        RequestParam requestParam=new RequestParam();
        requestParam.setUrl(url);
        return httpStreamingService.pollingRestApi(requestParam).flatMapIterable(s->{
            List<Trade> tradeList=new ArrayList<>();
            JsonNode jsonNode = mapper.readTree(s);
            String errorCode=jsonNode.findValue("errorCode").asText();
            if("0".equalsIgnoreCase(errorCode)) {
                ArrayNode transactionsNode=(ArrayNode)jsonNode.get("completeOrders");
                if(null!=transactionsNode){
                    for (JsonNode item:transactionsNode){
                        Trade trade=new Trade();
                        trade.setExchange("coinone");
                        trade.setCurrencyPair(currencyPair);
                        trade.setPrice(DataUtils.objToBigDecimal(item.get("price")));
                        trade.setOriginalAmount(DataUtils.objToBigDecimal(item.get("qty")));
                        trade.setTradeType(TradeTypeUtil.getOrderType(item.get("is_ask").asText().toLowerCase()));
                        trade.setTimestamp(DataUtils.objToLong(item.get("timestamp")));
                        trade.setTradeId(DataUtils.objToLong(item.get("id")));
                        tradeList.add(trade);
                    }
                }
            }
            return tradeList;
        });
    }

    @Override
    public Observable<Kline> getCurrentKLine(String currencyPair, Object... args) {
        return null;
    }

    @Override
    public Observable<Kline> getHistoryKLine(String currencyPair, Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

    @Override
    public Observable<OrderBook> getOrderBook(String currencyPair, Object... args) {
        String url="https://api.coinone.co.kr/orderbook/?currency="+currencyPair.split("-")[0];
        RequestParam requestParam=new RequestParam();
        requestParam.setUrl(url);
        return httpStreamingService.pollingRestApi(requestParam).map(s->{
            OrderBook orderBook=new OrderBook();
            orderBook.setExchange("coinone");
            orderBook.setCurrencyPair(currencyPair);
            JsonNode jsonNode = mapper.readTree(s);
            String errorCode=jsonNode.findValue("errorCode").asText();
            if("0".equalsIgnoreCase(errorCode)) {
                orderBook.setTimestamp(DataUtils.objToLong(jsonNode.get("timestamp")));
                ArrayNode bidsArray=(ArrayNode)jsonNode.get("bid");
                if(null!=bidsArray){
                    List<PriceAndVolume> bids=new ArrayList<>();
                    for (JsonNode bidItemNode:bidsArray){
                        PriceAndVolume priceAndVolume=new PriceAndVolume(DataUtils.objToBigDecimal(bidItemNode.get("price")),DataUtils.objToBigDecimal(bidItemNode.get("qty")));
                        bids.add(priceAndVolume);
                    }
                    if(bids.size()>20){
                        bids=bids.subList(0,20);
                    }
                    orderBook.setBids(bids);
                }
                ArrayNode asksArray=(ArrayNode)jsonNode.get("ask");
                if(null!=asksArray){
                    List<PriceAndVolume> asks=new ArrayList<>();
                    for (JsonNode askItemNode:asksArray){
                        PriceAndVolume priceAndVolume=new PriceAndVolume(DataUtils.objToBigDecimal(askItemNode.get("price")),DataUtils.objToBigDecimal(askItemNode.get("qty")));
                        asks.add(priceAndVolume);
                    }
                    if(asks.size()>20){
                        asks=asks.subList(0,20);
                    }
                    orderBook.setAsks(asks);
                }
            }
            return orderBook;
        });
    }
}
