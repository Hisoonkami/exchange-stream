package com.adev.exchange.stream.exchange.kraken;

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
import java.util.Iterator;

public class KrakenStreamingMarketDataService implements StreamingMarketDataService {

    private final HttpStreamingService httpStreamingService;
    private ObjectMapper mapper = new ObjectMapper();

    public KrakenStreamingMarketDataService(HttpStreamingService httpStreamingService) {
        this.httpStreamingService = httpStreamingService;
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Observable<Ticker> getTicker(String currencyPair, Object... args) {
        String pair=currencyPair.replace("-","");
        String url="https://api.kraken.com/0/public/Ticker?pair="+pair;
        RequestParam requestParam=new RequestParam();
        requestParam.setUrl(url);
        Observable<Ticker> observable=httpStreamingService.pollingRestApi(requestParam).map(s->{
            JsonNode jsonNode = mapper.readTree(s);
            Ticker ticker=new Ticker();
            ticker.setExchange("kraken");
            ticker.setCurrencyPair(currencyPair);
            JsonNode tickerNode = jsonNode.get("result").get(pair.toUpperCase());
            if(null==tickerNode){
                Iterator<JsonNode> it=jsonNode.get("result").iterator();
                if(null!=it&&it.hasNext()){
                    tickerNode=it.next();
                }
            }

            if(null==tickerNode){
                return ticker;
            }
            BigDecimal high=DataUtils.objToBigDecimal(tickerNode.get("h").get(1));
            ticker.setHigh(high);
            BigDecimal low=DataUtils.objToBigDecimal(tickerNode.get("l").get(1));
            ticker.setLow(low);
            BigDecimal last=DataUtils.objToBigDecimal(tickerNode.get("c").get(1));
            ticker.setLast(last);
            BigDecimal volume=DataUtils.objToBigDecimal(tickerNode.get("v").get(1));
            ticker.setVolume(volume);
            BigDecimal open=DataUtils.objToBigDecimal(tickerNode.get("o"));
            ticker.setOpen(open);
            ticker.setTimestamp(System.currentTimeMillis());
            return ticker;
        });
        return observable;
    }

    @Override
    public Observable<Trade> getTrades(String currencyPair, Object... args) {
        return null;
    }

    @Override
    public Observable<Kline> getCurrentKLine(String currencyPair, Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

    @Override
    public Observable<Kline> getHistoryKLine(String currencyPair, Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

    @Override
    public Observable<OrderBook> getOrderBook(String currencyPair, Object... args) {
        return null;
    }
}
