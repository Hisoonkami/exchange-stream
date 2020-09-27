package com.adev.exchange.stream.service.impl;

import com.adev.common.base.domian.BaseResult;
import com.adev.common.base.domian.CoinPairBase;
import com.adev.common.base.domian.Ticker;
import com.adev.common.base.enums.Enums;
import com.adev.common.exchange.StreamingExchange;
import com.adev.common.exchange.StreamingExchangeFactory;
import com.adev.exchange.stream.config.SubscribeConfig;
import com.adev.exchange.stream.service.GatherFeignClient;
import com.adev.exchange.stream.service.SubscribeService;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SubscribeServiceImpl implements SubscribeService {
    private static final Logger LOG = LoggerFactory.getLogger(SubscribeServiceImpl.class);

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private GatherFeignClient gatherFeignClient;

    @Override
    public void subscribe(SubscribeConfig.SubscribeExchange exchange) {
        List<String> currencyPairList=findPairByExchange(exchange.getName());
        if(null!=currencyPairList&&!currencyPairList.isEmpty()){
            StreamingExchange streamingExchange=getStreamingExchange(exchange);
            if(null!=streamingExchange){
                if(exchange.isAllTicker()){
                    streamingExchange.getStreamingMarketDataService().getTicker(null).subscribe(ticker -> {
                        if(currencyPairList.contains(ticker.getCurrencyPair())){
                            sendTickerToMq(ticker);
                        }
                    });
                }else {
                    for (String currencyPair:currencyPairList){
                        streamingExchange.getStreamingMarketDataService().getTicker(currencyPair).subscribe(ticker -> {
                            sendTickerToMq(ticker);
                        });
                    }
                }
                for (String currencyPair:currencyPairList){
                    streamingExchange.getStreamingMarketDataService().getOrderBook(currencyPair).subscribe(orderBook->{
                        System.out.println(orderBook);
                    });

                    streamingExchange.getStreamingMarketDataService().getTrades(currencyPair).subscribe(trade -> {
                        System.out.println(trade);
                    });
//                    try {
//                        Disposable disposable = streamingExchange.getStreamingMarketDataService().getTicker(coinPair.getCurrencyPair()).subscribe(xTicker -> {
//                            try {
//                                String channel="/topic/tickers/"+exchange.getName()+"/"+coinPair.getCurrencyPair();
//                                Map<String,Object> messageMap=new HashMap<>();
//                                messageMap.put("channel",channel);
//                                messageMap.put("message",xTicker);
//                                rabbitTemplate.convertAndSend("exchange","topic.ticker",JSON.toJSONString(messageMap));
//                                System.out.println(xTicker);
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }, throwable -> LOG.error("{}, ERROR in getting ticker1: ", exchange, throwable));
//
////                        streamingExchange.getStreamingMarketDataService().getDepth(coinPair.getCurrencyPair()).subscribe(xDepth->{
////                            String channel="/topic/depths/"+exchange.getName()+"/"+coinPair.getCurrencyPair();
////                            Map<String,Object> messageMap=new HashMap<>();
////                            messageMap.put("channel",channel);
////                            messageMap.put("message",xDepth);
////                            rabbitTemplate.convertAndSend("exchange","topic.depth",JSON.toJSONString(messageMap));
////                            System.out.println(xDepth);
////                        });
////                        streamingExchange.getStreamingMarketDataService().getTrades(coinPair.getCurrencyPair()).subscribe(xTrade->{
////                            String channel="/topic/trades/"+exchange.getName()+"/"+coinPair.getCurrencyPair();
////                            Map<String,Object> messageMap=new HashMap<>();
////                            messageMap.put("channel",channel);
////                            messageMap.put("message",xTrade);
////                            rabbitTemplate.convertAndSend("exchange","topic.trade",JSON.toJSONString(messageMap));
////                            System.out.println(xTrade);
////                        });
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
                }
            }
        }
    }

    private void sendTickerToMq(Ticker ticker){
        String channel="/topic/tickers/"+ticker.getExchange()+"/"+ticker.getCurrencyPair();
        Map<String,Object> messageMap=new HashMap<>();
        messageMap.put("channel",channel);
        messageMap.put("message",ticker);
        LOG.info("send message:{}",ticker);
        System.out.println(ticker);
        rabbitTemplate.convertAndSend("exchange","topic.ticker",JSON.toJSONString(messageMap));
    }

    private List<String> findPairByExchange(String exchange){
        ResponseEntity<BaseResult> resultResponseEntity= gatherFeignClient.findCurrencyPairByExchange(exchange);
        if(null!=resultResponseEntity){
            Object data=resultResponseEntity.getBody().getData();
            if(null!=data){
                List<String> currencyPairList=new ArrayList<>();
                List<Object> dataList=(List)data;
                for (Object dataItem:dataList){
                    Map<String,Object> dataMap=(Map)dataItem;
                    currencyPairList.add(String.valueOf(dataMap.get("pairName")));
                }
                return currencyPairList;
            }
        }
        return null;
    }

    private StreamingExchange getStreamingExchange(SubscribeConfig.SubscribeExchange exchange){
        StreamingExchange streamingExchange=StreamingExchangeFactory.INSTANCE.createExchange(exchange.getClassName());
        if(null!=streamingExchange){
            streamingExchange.connect(null).blockingAwait();
        }
        return streamingExchange;
    }
}
