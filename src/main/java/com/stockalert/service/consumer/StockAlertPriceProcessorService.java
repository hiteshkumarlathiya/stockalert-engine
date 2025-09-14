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

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class StockAlertPriceProcessorService {

	@Autowired
	private StockAlertRegistry alertRegistry;

	@Autowired
	private StockAlertTriggerPublisher dispatcher;

	@KafkaListener(topics = "${confluent.topic.market.stock.prices}", groupId = "alert-evaluator", containerFactory = "kafkaListenerContainerFactory")
	public void evaluate(ConsumerRecord<String, StockPriceEvent> record, Acknowledgment ack) {
		String symbol = null;
		try {
			log.info("Thread name:{} :: Stock key: {}, value: {}, partition: {}", 
					Thread.currentThread().getName(), record.key(), record.value(), record.partition());
			StockPriceEvent event = record.value();
			symbol = event.getSymbol();
			// Ensure symbol is loaded by price proximity (hybrid)
			long findBucketTime1  = System.currentTimeMillis();
			NavigableMap<Double, List<UserStockAlertDTO>> buckets = alertRegistry.getAlertsForSymbol(event.getSymbol());
			if (buckets.isEmpty()) {
				alertRegistry.loadSymbolNearPrice(event.getSymbol(), event.getPrice());
				buckets = alertRegistry.getAlertsForSymbol(event.getSymbol());
			}
			log.info("Get Bucket for symbol: {}, time: {}", event.getSymbol(), (System.currentTimeMillis() - findBucketTime1));

			if (buckets.isEmpty()) {
				ack.acknowledge();
				return;
			}

			// Process alerts for this symbol
			List<Long> triggerdAlertIds = processPrice(event.getSymbol(), event.getPrice());

			if(!CollectionUtils.isEmpty(triggerdAlertIds)) {
				alertRegistry.updateTriggeredAlerts(event.getSymbol(), triggerdAlertIds);
			}
			// Only acknowledge after successful processing
			ack.acknowledge();
		} catch (Exception ex) {
			log.error("Error processing event for symbol {}: {}", symbol, ex.getMessage(), ex);
			// Do NOT ack here — message will be retried depending on your error handler
		}
	}

	public List<Long> processPrice(String symbol, double price) {
		long time = System.currentTimeMillis();

		List<Long> triggerdAlertIds = new ArrayList<>();

		NavigableMap<Double, List<UserStockAlertDTO>> buckets = alertRegistry.getAlertsForSymbol(symbol);
		if (buckets == null || buckets.isEmpty()) return triggerdAlertIds;

		// collect all alerts having threshold value < CURRENT(inclusive) for ABOVE triggers
		long abovetime = System.currentTimeMillis();
		buckets.headMap(price, false).forEach((threshold, alerts) -> {
			for (UserStockAlertDTO alert : alerts) {
				if (shouldTrigger(alert, TriggerType.ABOVE, price)) {
					dispatchAndMark(alert, price);
					triggerdAlertIds.add(alert.getAlertId());
				}
			}
		});
		log.info("ABOVE trigger type processed for symbol: {}, time: {}", symbol, (System.currentTimeMillis() - abovetime));
		// collect all alerts having threshold value > CURRENT(inclusive) for BELOW triggers
		long belowTime = System.currentTimeMillis();
		buckets.tailMap(price, false).forEach((threshold, alerts) -> {
			for (UserStockAlertDTO alert : alerts) {
				if (shouldTrigger(alert, TriggerType.BELOW, price)) {
					dispatchAndMark(alert, price);
					triggerdAlertIds.add(alert.getAlertId());
				}
			}
		});
		log.info("BELOW trigger type processed for symbol: {}, time: {}", symbol, (System.currentTimeMillis() - belowTime));
		log.info("Process done for symbol: {}, time: {}, bucket_size: {}", symbol, (System.currentTimeMillis() - time), buckets.size());
		return triggerdAlertIds;
	}

	private boolean shouldTrigger(UserStockAlertDTO alert, TriggerType type, double price) {
		return alert.isActive()
				&& !alert.isTriggered()
				&& alert.getTriggerType() == type;
	}

	private void dispatchAndMark(UserStockAlertDTO alert, double price) {
		alert.setTriggered(true);
		alert.setActive(false);
		dispatcher.markAsTriggered(alert, price);
		dispatcher.notify(alert, price);
	}
}