package com.stockalert.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.stockalert.common.AlertChangeEvent;
import com.stockalert.service.util.StockAlertRegistry;
import com.stockalert.service.util.SymbolEventExecutor;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AlertChangeConsumer {

    @Autowired
    private StockAlertRegistry registry;

    @Autowired
    private SymbolEventExecutor symbolEventExecutor;

    @KafkaListener(
    	    topics = "${confluent.topic.alert.changes}",
    	    groupId = "alert-change-applier",
    	    containerFactory = "alertChangeKafkaListenerContainerFactory"
    	)
    public void onAlertChange(ConsumerRecord<String, AlertChangeEvent> record, Acknowledgment ack) {
        AlertChangeEvent evt = record.value();
        String symbol = evt.getSymbol();

        // Submit to per-symbol executor to preserve ordering with price events
        symbolEventExecutor.submit(symbol, () -> {
            try {
                log.info("Processing AlertChangeEvent for symbol={} type={} alertId={} partition={} offset={}",
                        symbol, evt.getType(), evt.getAlertId(),
                        record.partition(), record.offset());

                registry.applyAlertChange(evt);

                ack.acknowledge();
            } catch (Exception e) {
                log.error("Failed to apply alert change for {}: {}", symbol, e.getMessage(), e);
            }
        });
    }
}