package com.stockalert.service.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

@Component
public class HotSymbolTracker {
	
	private final Set<String> hot = ConcurrentHashMap.newKeySet();

	public void markHot(String symbol) {
		hot.add(symbol);
	}

	public boolean isHot(String symbol) {
		return hot.contains(symbol);
	}

	public void clear(String symbol) {
		hot.remove(symbol);
	}
}