package com.adev.exchange.stream.exchange.coinone;

import com.adev.common.base.domian.Kline;
import com.adev.common.base.domian.OrderBook;
import com.adev.common.base.domian.Ticker;
import com.adev.common.base.domian.Trade;
import com.adev.common.base.utils.DataUtils;
import com.adev.common.exchange.StreamingMarketDataService;
import com.adev.common.exchange.exception.NotYetImplementedForExchangeException;
import com.adev.common.exchange.http.domain.RequestParam;
import com.adev.common.exchange.http.service.HttpStreamingService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        return null;
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
        throw new NotYetImplementedForExchangeException();
    }
}
