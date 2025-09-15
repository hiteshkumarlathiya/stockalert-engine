package com.stockalert.service.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.stockalert.api.client.UserStockAlertClient;
import com.stockalert.cache.StockAlertCache;
import com.stockalert.common.AlertChangeEvent;
import com.stockalert.common.PriceWindow;
import com.stockalert.common.UserStockAlertDTO;
import com.stockalert.util.SymbolPriceRanges;

// Optional (only if you wire user change events)
// import com.stockalert.common.AlertChangeEvent;
// import com.stockalert.common.AlertChangeType;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class StockAlertRegistry {

    // Local in-memory cache: symbol -> immutable(sorted thresholds -> immutable list of alerts)
    private final Map<String, NavigableMap<Double, List<UserStockAlertDTO>>> alertBuckets = new ConcurrentHashMap<>();
    // Access timestamps for eviction
    private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();
    // Loaded price range per symbol
    private final Map<String, SymbolPriceRanges.Range> loadedRanges = new ConcurrentHashMap<>();
    // Symbol-level write locks (reads are lock-free on immutable snapshots)
    private final Map<String, ReentrantLock> symbolLocks = new ConcurrentHashMap<>();

    @Autowired
    private UserStockAlertClient userStockAlertClient;

    @Autowired
    private StockAlertCache cache;

    // ---------------- Public API ----------------

    public NavigableMap<Double, List<UserStockAlertDTO>> getAlertsForSymbol(String symbol) {
        NavigableMap<Double, List<UserStockAlertDTO>> m = alertBuckets.get(symbol);
        if (m != null) {
            touch(symbol);
            return m; // already immutable
        }
        // return immutable empty map to avoid NPE and preserve read-only contract
        return Collections.unmodifiableNavigableMap(new ConcurrentSkipListMap<>());
    }

    /**
     * Preferred API: Ensure alerts for a symbol are loaded for the given price band.
     * If the price moves outside the current loaded range, it evicts Redis for this symbol (to avoid stale band),
     * reloads from Redis or DB, and atomically swaps local snapshot.
     */
    public void ensureSymbolLoadedForPrice(String symbol, double price) {
        SymbolPriceRanges.Range current = loadedRanges.get(symbol);
        double min = PriceWindow.lower(price);
        double max = PriceWindow.upper(price);

        boolean needsReload = current == null || price < current.min || price > current.max;
        if (!needsReload) {
            touch(symbol);
            return;
        }

        withSymbolLock(symbol, () -> {
            // Check again under lock to avoid redundant reload
            SymbolPriceRanges.Range latest = loadedRanges.get(symbol);
            boolean stillNeedsReload = latest == null || price < latest.min || price > latest.max;
            if (!stillNeedsReload) {
                touch(symbol);
                return;
            }

            // Invalidate Redis if the price band has shifted
            if (latest != null && (latest.min != min || latest.max != max)) {
                cache.evict(symbol);
                log.info("Evicted Redis cache for {} due to range change: old={} - {}, new={} - {}",
                        symbol, latest.min, latest.max, min, max);
            }

            // Try Redis first, then DB
            List<UserStockAlertDTO> alerts = cache.get(symbol);
            if (alerts == null) {
                alerts = userStockAlertClient.getAlertsBySymbolAndThresholdRange(symbol, min, max);
            }

            if (!CollectionUtils.isEmpty(alerts)) {
                buildAndSwap(symbol, alerts);
                cache.put(symbol, alerts); // refresh TTL
                loadedRanges.put(symbol, new SymbolPriceRanges.Range(min, max));
            } else {
                // No alerts for this band: clear local, but set the range so we don't hammer reloads
                alertBuckets.remove(symbol);
                loadedRanges.put(symbol, new SymbolPriceRanges.Range(min, max));
                touch(symbol);
            }
        });
    }

    /**
     * Backward-compatible with your previous code: delegates to ensureSymbolLoadedForPrice.
     */
    public void loadSymbolNearPrice(String symbol, double price) {
        ensureSymbolLoadedForPrice(symbol, price);
    }

    /**
     * Replace local snapshot with the provided alerts (filters inactive) and update last access.
     * Caller should ensure correct price band is used; typical callers are ensureSymbolLoadedForPrice/applyAlertChange.
     */
    public void loadSymbol(String symbol, List<UserStockAlertDTO> alerts) {
        withSymbolLock(symbol, () -> {
            buildAndSwap(symbol, alerts);
        });
    }

    /**
     * Remove triggered alerts for the symbol (batch), updating local snapshot and Redis atomically.
     */
    public void updateTriggeredAlerts(String symbol, List<Long> triggeredAlertIds) {
        if (CollectionUtils.isEmpty(triggeredAlertIds)) return;

        withSymbolLock(symbol, () -> {
            // Start from Redis to ensure we reflect the last persisted view
            List<UserStockAlertDTO> base = cache.get(symbol);
            if (base == null) {
                base = flatten(alertBuckets.get(symbol));
            }
            if (CollectionUtils.isEmpty(base)) {
                alertBuckets.remove(symbol);
                cache.evict(symbol);
                return;
            }

            List<UserStockAlertDTO> remaining = base.stream()
                    .filter(a -> !triggeredAlertIds.contains(a.getAlertId()))
                    .collect(Collectors.toList());

            if (remaining.isEmpty()) {
                alertBuckets.remove(symbol);
                cache.evict(symbol);
            } else {
                buildAndSwap(symbol, remaining);
                cache.put(symbol, remaining);
            }
        });
    }

    /**
     * Evict everything for a symbol (local + Redis + range + LRU).
     */
    public void evictSymbol(String symbol) {
        withSymbolLock(symbol, () -> {
            log.info("Evict Symbol: {}", symbol);
            alertBuckets.remove(symbol);
            cache.evict(symbol);
            loadedRanges.remove(symbol);
            lastAccess.remove(symbol);
        });
    }

    public List<String> symbolsIdleLongerThan(long millis) {
        long cutoff = System.currentTimeMillis() - millis;
        return lastAccess.entrySet()
                .stream()
                .filter(e -> e.getValue() < cutoff)
                .map(Map.Entry::getKey)
                .toList();
    }

    public int totalLocalAlerts() {
        return alertBuckets.values().stream()
                .mapToInt(m -> m.values().stream().mapToInt(List::size).sum())
                .sum();
    }

    public void applyAlertChange(AlertChangeEvent evt) {
        String symbol = evt.getSymbol();
        withSymbolLock(symbol, () -> {
            List<UserStockAlertDTO> base = cache.get(symbol);
            if (base == null) {
                base = flatten(alertBuckets.get(symbol));
            }
            List<UserStockAlertDTO> updated = base != null ? new ArrayList<>(base) : new ArrayList<>();

            switch (evt.getType()) {
                case CREATE, UPDATE -> {
                    if (evt.getAlert() != null) {
                        updated.removeIf(a -> Objects.equals(a.getAlertId(), evt.getAlert().getAlertId()));
                        updated.add(evt.getAlert());
                    }
                }
                case INACTIVATE, DELETE -> {
                    if (evt.getAlertId() != null) {
                        updated.removeIf(a -> Objects.equals(a.getAlertId(), evt.getAlertId()));
                    }
                }
                default -> {}
            }

            if (updated.isEmpty()) {
                alertBuckets.remove(symbol);
                cache.evict(symbol);
            } else {
                buildAndSwap(symbol, updated);
                cache.put(symbol, updated);
            }

            // If new thresholds are outside current range, force a reload next time
            SymbolPriceRanges.Range lr = loadedRanges.get(symbol);
            if (lr != null) {
                boolean outOfBand = updated.stream()
                        .anyMatch(a -> a.getThreshold() < lr.min || a.getThreshold() > lr.max);
                if (outOfBand) {
                    loadedRanges.remove(symbol);
                }
            }
        });
    }

    // ---------------- Internals ----------------

    private void buildAndSwap(String symbol, List<UserStockAlertDTO> alerts) {
        // Build fresh, filter inactive
        NavigableMap<Double, List<UserStockAlertDTO>> temp = new ConcurrentSkipListMap<>();
        if (!CollectionUtils.isEmpty(alerts)) {
            for (UserStockAlertDTO a : alerts) {
                if (a == null || !a.isActive()) continue;
                temp.computeIfAbsent(a.getThreshold(), t -> new ArrayList<>()).add(a);
            }
        }

        // Freeze lists and map
        NavigableMap<Double, List<UserStockAlertDTO>> frozen = new ConcurrentSkipListMap<>();
        temp.forEach((k, v) -> frozen.put(k, List.copyOf(v)));

        // Atomic replacement (immutable snapshot)
        alertBuckets.put(symbol, Collections.unmodifiableNavigableMap(frozen));
        touch(symbol);
    }

    private List<UserStockAlertDTO> flatten(NavigableMap<Double, List<UserStockAlertDTO>> map) {
        if (map == null) return null;
        return map.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private void touch(String symbol) {
        lastAccess.put(symbol, System.currentTimeMillis());
    }

    private ReentrantLock lockFor(String symbol) {
        return symbolLocks.computeIfAbsent(symbol, k -> new ReentrantLock());
    }

    private void withSymbolLock(String symbol, Runnable r) {
        ReentrantLock lock = lockFor(symbol);
        lock.lock();
        try {
            r.run();
        } finally {
            lock.unlock();
        }
    }
}