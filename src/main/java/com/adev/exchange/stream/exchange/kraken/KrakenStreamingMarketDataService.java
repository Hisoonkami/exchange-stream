package com.adev.exchange.stream.exchange.kraken;

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
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        String pair=currencyPair.replace("-","");
        String url="https://api.kraken.com/0/public/Trades?pair="+pair;
        RequestParam requestParam=new RequestParam();
        requestParam.setUrl(url);
        return httpStreamingService.pollingRestApi(requestParam).flatMapIterable(s -> {
            List<Trade> tradeList=new ArrayList<>();
            JsonNode jsonNode = mapper.readTree(s);
            ArrayNode tradesNode = (ArrayNode)jsonNode.get("result").get(pair.toUpperCase());
            if(null!=tradesNode){
                for (JsonNode tradeNode:tradesNode){
                    ArrayNode tradeNodeArray=(ArrayNode)tradeNode;
                    Trade trade=new Trade();
                    trade.setExchange("kraken");
                    trade.setCurrencyPair(currencyPair);
                    trade.setPrice(DataUtils.objToBigDecimal(tradeNodeArray.get(0)));
                    trade.setOriginalAmount(DataUtils.objToBigDecimal(tradeNodeArray.get(1)));
                    BigDecimal d=BigDecimal.valueOf(tradeNodeArray.get(2).asDouble());
                    String timestampStr=d.toPlainString();
                    if(StringUtils.isNotBlank(timestampStr)){
                        String[] timestampArr=timestampStr.split("\\.");
                        trade.setTimestamp(new Long(timestampArr[0]+"000"));
                        trade.setTradeId(new Long(timestampArr[0]+timestampArr[1]));
                    }
                    trade.setTradeType(TradeTypeUtil.getOrderType(tradeNodeArray.get(3).asText().toLowerCase()));
                    tradeList.add(trade);
                }
            }
            return tradeList;
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

    @Override
    public Observable<OrderBook> getOrderBook(String currencyPair, Object... args) {
        String pair=currencyPair.replace("-","");
        String url="https://api.kraken.com/0/public/Depth?pair="+pair+"&count=20";
        RequestParam requestParam=new RequestParam();
        requestParam.setUrl(url);
        return httpStreamingService.pollingRestApi(requestParam).map(s->{
            OrderBook orderBook=new OrderBook();
            JsonNode jsonNode = mapper.readTree(s);
            orderBook.setExchange("kraken");
            orderBook.setCurrencyPair(currencyPair);
            JsonNode orderBookNode = jsonNode.get("result").get(pair.toUpperCase());
            if(null!=orderBookNode){
                Long timestamp=null;
                ArrayNode asksArray=(ArrayNode)orderBookNode.get("asks");
                if(null!=asksArray){
                    List<PriceAndVolume> asks=new ArrayList<>();
                    for (JsonNode askItem:asksArray){
                        ArrayNode askItemArray=(ArrayNode)askItem;
                        PriceAndVolume priceAndVolume=new PriceAndVolume(DataUtils.objToBigDecimal(askItemArray.get(0)),DataUtils.objToBigDecimal(askItemArray.get(1)));
                        asks.add(priceAndVolume);
                        if(null==timestamp){
                            timestamp=DataUtils.objToLong(askItemArray.get(2)+"000");
                        }
                    }
                    if(asks.size()>20){
                        asks=asks.subList(0,20);
                    }
                    orderBook.setAsks(asks);
                }
                ArrayNode bidsArray=(ArrayNode)orderBookNode.get("bids");
                if(null!=bidsArray){
                    List<PriceAndVolume> bids=new ArrayList<>();
                    for (JsonNode bidItem:bidsArray){
                        ArrayNode bidItemArray=(ArrayNode)bidItem;
                        PriceAndVolume priceAndVolume=new PriceAndVolume(DataUtils.objToBigDecimal(bidItem.get(0)),DataUtils.objToBigDecimal(bidItem.get(1)));
                        bids.add(priceAndVolume);
                        if(null==timestamp){
                            timestamp=DataUtils.objToLong(bidItemArray.get(2)+"000");
                        }
                    }
                    if(bids.size()>20){
                        bids=bids.subList(0,20);
                    }
                    orderBook.setBids(bids);
                }
                orderBook.setTimestamp(timestamp);
            }
            return orderBook;
        });
    }
}
