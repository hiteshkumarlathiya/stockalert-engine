package com.stockalert.service.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.stockalert.api.client.UserStockAlertClient;
import com.stockalert.cache.StockAlertCache;
import com.stockalert.common.PriceWindow;
import com.stockalert.common.UserStockAlertDTO;
import com.stockalert.util.SymbolPriceRanges;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class StockAlertRegistry {

	// Local cache: symbol -> threshold -> alerts (sorted thresholds)
	private final Map<String, NavigableMap<Double, List<UserStockAlertDTO>>> alertBuckets = new ConcurrentHashMap<>();
	// Access timestamps for eviction
	private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();
	private final Map<String, SymbolPriceRanges.Range> loadedRanges = new ConcurrentHashMap<>();

	@Autowired
	private UserStockAlertClient userStockAlertClient;
	
	@Autowired
	private StockAlertCache cache;

	public NavigableMap<Double, List<UserStockAlertDTO>> getAlertsForSymbol(String symbol) {
		NavigableMap<Double, List<UserStockAlertDTO>> m = alertBuckets.get(symbol);
		if (m != null) {
			touch(symbol);
			return m;
		}
		return new ConcurrentSkipListMap<>();
		// Note: fetching happens via explicit loaders below
	}

	// Load on-demand near price: local miss -> Redis -> DB by price window
	public void loadSymbolNearPrice(String symbol, double price) {
        SymbolPriceRanges.Range currentRange = loadedRanges.get(symbol);
        
        if (currentRange == null || price < currentRange.min || price > currentRange.max) {

            double min = PriceWindow.lower(price);
            double max = PriceWindow.upper(price);

            // Invalidate Redis cache if range is changing
            if (currentRange != null && (min != currentRange.min || max != currentRange.max)) {
                cache.evict(symbol);
                log.info("Evicted Redis cache for {} due to range change: old={} - {}, new={} - {}",
                          symbol, currentRange.min, currentRange.max, min, max);
            }

            // Try Redis (might be empty if just evicted)
            List<UserStockAlertDTO> alerts = cache.get(symbol);
            if (alerts == null) {
                alerts = userStockAlertClient.getAlertsBySymbolAndThresholdRange(symbol, min, max);
            }

            if (alerts != null && !alerts.isEmpty()) {
                loadSymbol(symbol, alerts); // local memory
                cache.put(symbol, alerts);  // refresh Redis TTL
                loadedRanges.put(symbol, new SymbolPriceRanges.Range(min, max));
            }
        } else {
            touch(symbol);
        }

        // If no range loaded yet OR price outside current range → reload
        if (currentRange == null || price < currentRange.min || price > currentRange.max) {
            double min = PriceWindow.lower(price);
            double max = PriceWindow.upper(price);

            List<UserStockAlertDTO> alerts = userStockAlertClient.getAlertsBySymbolAndThresholdRange(symbol, min, max);

            if (alerts != null && !alerts.isEmpty()) {
            	cache.put(symbol, alerts);
                loadSymbol(symbol, alerts);
                loadedRanges.put(symbol, new SymbolPriceRanges.Range(min, max));
            }
        } else {
            // Already loaded and price is in range — just touch for eviction tracking
            touch(symbol);
        }
    }

	public void loadSymbol(String symbol, List<UserStockAlertDTO> alerts) {
		NavigableMap<Double, List<UserStockAlertDTO>> buckets = new ConcurrentSkipListMap<>();
		for (UserStockAlertDTO a : alerts) {
			if (!a.isActive())
				continue;
			buckets.computeIfAbsent(a.getThreshold(), t -> new ArrayList<>()).add(a);
		}
		alertBuckets.put(symbol, buckets);
		touch(symbol);
	}

	public void evictSymbol(String symbol) {
		log.info("Evict Symbol: {}", symbol);
		loadedRanges.remove(symbol);
		alertBuckets.remove(symbol);
		lastAccess.remove(symbol);
	}

	public List<String> symbolsIdleLongerThan(long millis) {
		long cutoff = System.currentTimeMillis() - millis;
		return lastAccess.entrySet().stream().filter(e -> e.getValue() < cutoff).map(Map.Entry::getKey).toList();
	}

	public int totalLocalAlerts() {
		return alertBuckets.values().stream().mapToInt(m -> m.values().stream().mapToInt(List::size).sum()).sum();
	}

	private void touch(String symbol) {
		lastAccess.put(symbol, System.currentTimeMillis());
	}

	public void updateTriggeredAlerts(String symbol, List<Long> triggerdAlertIds) {
		List<UserStockAlertDTO> alerts = cache.get(symbol);
		if (CollectionUtils.isEmpty(alerts)) return;
		this.evictSymbol(symbol);
		List<UserStockAlertDTO> updated = alerts.stream()
				.filter(o -> !triggerdAlertIds.contains(o.getAlertId())) //remove triggered alerts
				.collect(Collectors.toList());

		if (updated.isEmpty()) {
			cache.evict(symbol);
		} else {
			cache.put(symbol, updated);
		}
	}
}