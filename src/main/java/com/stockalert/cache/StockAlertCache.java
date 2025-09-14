package com.stockalert.cache;

import java.time.Duration;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.stockalert.common.UserStockAlertDTO;

@Component
public class StockAlertCache {

    private static final Duration TTL = Duration.ofMinutes(20);
    private static final String PREFIX = "alerts:";

    @Autowired
    private RedisTemplate<String, Object> redis;

    public void put(String symbol, List<UserStockAlertDTO> alerts) {
        redis.opsForValue().set(PREFIX + symbol, alerts, TTL);
    }

    @SuppressWarnings("unchecked")
    public List<UserStockAlertDTO> get(String symbol) {
        Object v = redis.opsForValue().get(PREFIX + symbol);
        return v == null ? null : (List<UserStockAlertDTO>) v;
    }

    public void evict(String symbol) {
        redis.delete(PREFIX + symbol);
    }
}