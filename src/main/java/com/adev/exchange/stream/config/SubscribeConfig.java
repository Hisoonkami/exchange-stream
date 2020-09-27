package com.adev.exchange.stream.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Data
@RefreshScope // 加了这个注解，当外部配置刷新的时候，这里的数据就会同步刷新
@ConfigurationProperties(prefix = "subscribe")
public class SubscribeConfig {
    private List<SubscribeExchange> exchanges;

    @Data
    @NoArgsConstructor
    public static class SubscribeExchange{
        private String name;
        private String className;
        private boolean allTicker=false;
    }
}
