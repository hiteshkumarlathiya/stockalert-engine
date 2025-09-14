package com.stockalert.service.util;

import java.util.ArrayList;
import java.util.Collection;
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

@Component
public class StockAlertRegistry {

	// Local cache: symbol -> threshold -> alerts (sorted thresholds)
	private final Map<String, NavigableMap<Double, List<UserStockAlertDTO>>> alertBuckets = new ConcurrentHashMap<>();
	// Access timestamps for eviction
	private final Map<String, Long> lastAccess = new ConcurrentHashMap<>();

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

	// Preload full symbol set (used at startup for top symbols)
	public void loadAllForSymbols(Collection<String> symbols) {
		if (symbols == null || symbols.isEmpty())
			return;
		List<UserStockAlertDTO> alerts = userStockAlertClient.getAlertsBySymbols(new ArrayList<>(symbols));
		Map<String, List<UserStockAlertDTO>> bySymbol = alerts.stream()
				.collect(Collectors.groupingBy(UserStockAlertDTO::getSymbol));
		bySymbol.forEach((sym, list) -> {
			loadSymbol(sym, list);
			cache.put(sym, list);
		});
	}

	// Load on-demand near price: local miss -> Redis -> DB by price window
	public void loadSymbolNearPrice(String symbol, double price) {
		if (alertBuckets.containsKey(symbol)) {
			touch(symbol);
			return;
		}

		List<UserStockAlertDTO> alerts = cache.get(symbol);
		if (alerts == null) {
			double min = PriceWindow.lower(price);
			double max = PriceWindow.upper(price);
			alerts = userStockAlertClient.getAlertsBySymbolAndThresholdRange(symbol, min, max);
		}
		if (alerts == null || alerts.isEmpty())
			return;

		loadSymbol(symbol, alerts);
		cache.put(symbol, alerts);
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
