package com.stockalert.service.util;

import java.util.Map;
import java.util.concurrent.*;

import org.springframework.stereotype.Component;

@Component
public class SymbolEventExecutor {
    private final Map<String, ExecutorService> executors = new ConcurrentHashMap<>();

    public void submit(String symbol, Runnable task) {
        ExecutorService es = executors.computeIfAbsent(symbol, s ->
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r);
                t.setName("symbol-worker-" + s);
                t.setDaemon(true);
                return t;
            })
        );
        es.submit(task);
    }
}