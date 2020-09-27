package com.adev.exchange.stream;

import com.adev.exchange.stream.config.SubscribeConfig;
import com.adev.exchange.stream.service.SubscribeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class StartupRunner implements ApplicationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(StartupRunner.class);
    @Autowired
    private SubscribeConfig subscribeConfig;

    @Autowired
    private SubscribeService subscribeService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<SubscribeConfig.SubscribeExchange> subscribeExchanges=subscribeConfig.getExchanges();
        LOG.info("subscribeExchanges:{}",subscribeExchanges);
        if(null!=subscribeExchanges&&!subscribeExchanges.isEmpty()){
            for (SubscribeConfig.SubscribeExchange subscribeExchange:subscribeExchanges){
                subscribeService.subscribe(subscribeExchange);
            }
        }
    }


}
