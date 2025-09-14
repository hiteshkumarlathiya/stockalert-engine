package com.stockalert.service.util;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.stockalert.util.SymbolPreviousClose;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@EnableScheduling
public class StockAlertSchedulers {

    @Autowired private StockAlertRegistry registry;

    // Evict symbols idle > 10 minutes
    @Scheduled(fixedDelay = 120_000) // every 2 minutes
    public void evictIdle() {
    	log.info("evictIdle is invoked");
    	long time1 = System.currentTimeMillis();
        List<String> cold = registry.symbolsIdleLongerThan(10 * 60 * 1000L);
        cold.forEach(registry::evictSymbol);
        long time2 = System.currentTimeMillis();
        log.info("evictIdle is completed: {}", (time2 - time1));
    }

    @Scheduled(initialDelay = 0, fixedDelay = Long.MAX_VALUE) // every 5 minutes
    public void preloadSymbolPreviousClose() {
        log.info("preloadSymbolPreviousClose is invoked");
        long time1 = System.currentTimeMillis();

        var top = List.of("RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "SBIN",
                          "INFY", "TATAMOTORS", "ITC", "LT", "BHARTIARTL");

        top.forEach(symbol -> {
            Double closePrice = SymbolPreviousClose.getClosePrice(symbol);
            registry.loadSymbolNearPrice(symbol, closePrice);
        });
        long time2 = System.currentTimeMillis();
        log.info("preloadSymbolPreviousClose is completed: {}", (time2 - time1));
    }
}