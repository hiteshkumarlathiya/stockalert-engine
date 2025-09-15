package com.stockalert.service.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.stockalert.common.StockPriceEvent;
import com.stockalert.common.TriggerType;
import com.stockalert.common.UserStockAlertDTO;
import com.stockalert.service.publisher.StockAlertTriggerPublisher;
import com.stockalert.service.util.StockAlertRegistry;
import com.stockalert.service.util.SymbolEventExecutor;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class StockAlertPriceProcessorService {

	@Autowired
	private StockAlertRegistry alertRegistry;

	@Autowired
	private StockAlertTriggerPublisher dispatcher;

	@Autowired
	private SymbolEventExecutor symbolEventExecutor;

	@KafkaListener(topics = "${confluent.topic.market.stock.prices}", groupId = "alert-evaluator", containerFactory = "kafkaListenerContainerFactory")
	public void evaluate(ConsumerRecord<String, StockPriceEvent> record, Acknowledgment ack) {
		StockPriceEvent event = record.value();
		String symbol = event.getSymbol();
		double price = event.getPrice();

		// Submit to per-symbol executor to preserve ordering for that symbol
		symbolEventExecutor.submit(symbol, () -> {
			try {
				log.info("Thread={} key={} event={} partition={}", Thread.currentThread().getName(), record.key(),
						event, record.partition());

				// Ensure correct price band is loaded (handles Redis eviction + reload)
				long t0 = System.currentTimeMillis();
				alertRegistry.ensureSymbolLoadedForPrice(symbol, price);
				NavigableMap<Double, List<UserStockAlertDTO>> buckets = alertRegistry.getAlertsForSymbol(symbol);
				log.info("Loaded buckets for {} in {} ms (empty={})", symbol, (System.currentTimeMillis() - t0),
						buckets == null || buckets.isEmpty());

				if (buckets == null || buckets.isEmpty()) {
					ack.acknowledge();
					return;
				}

				// Evaluate triggers
				List<Long> triggeredAlertIds = processPrice(symbol, price, buckets);

				// Remove triggered alerts atomically (local + Redis)
				if (!CollectionUtils.isEmpty(triggeredAlertIds)) {
					alertRegistry.updateTriggeredAlerts(symbol, triggeredAlertIds);
				}

				// Ack only after successful processing
				ack.acknowledge();
			} catch (Exception ex) {
				log.error("Error processing event for symbol {}: {}", symbol, ex.getMessage(), ex);
				// No ack — let error handler retry
			}
		});
	}

	private List<Long> processPrice(String symbol, double price,
			NavigableMap<Double, List<UserStockAlertDTO>> buckets) {

		long tStart = System.currentTimeMillis();
		List<Long> triggeredAlertIds = new ArrayList<>();

		if (buckets == null || buckets.isEmpty())
			return triggeredAlertIds;

		// ABOVE triggers: thresholds < current price
		long tAbove = System.currentTimeMillis();
		buckets.headMap(price, false).forEach((threshold, alerts) -> {
			for (UserStockAlertDTO alert : alerts) {
				if (shouldTrigger(alert, TriggerType.ABOVE)) {
					dispatchAndMark(alert, price);
					triggeredAlertIds.add(alert.getAlertId());
				}
			}
		});
		log.info("ABOVE processed for {} in {} ms", symbol, (System.currentTimeMillis() - tAbove));

		// BELOW triggers: thresholds > current price
		long tBelow = System.currentTimeMillis();
		buckets.tailMap(price, false).forEach((threshold, alerts) -> {
			for (UserStockAlertDTO alert : alerts) {
				if (shouldTrigger(alert, TriggerType.BELOW)) {
					dispatchAndMark(alert, price);
					triggeredAlertIds.add(alert.getAlertId());
				}
			}
		});
		log.info("BELOW processed for {} in {} ms", symbol, (System.currentTimeMillis() - tBelow));

		log.info("Process done for {} in {} ms, bucket_size={}, triggered_count={}", symbol,
				(System.currentTimeMillis() - tStart), buckets.size(), triggeredAlertIds.size());

		return triggeredAlertIds;
	}

	private boolean shouldTrigger(UserStockAlertDTO alert, TriggerType type) {
		return alert.isActive() && !alert.isTriggered() && alert.getTriggerType() == type;
	}

	private void dispatchAndMark(UserStockAlertDTO alert, double price) {
		// Mark to avoid double-processing in this batch
		alert.setTriggered(true);
		alert.setActive(false);

		// Publish downstream
		dispatcher.markAsTriggered(alert, price);
		dispatcher.notify(alert, price);
	}
}