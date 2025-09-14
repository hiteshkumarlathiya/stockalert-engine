package com.stockalert.service.publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.stockalert.api.client.UserStockAlertClient;
import com.stockalert.common.UserStockAlertDTO;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@Async
public class StockAlertTriggerPublisher {
	
	@Autowired
	private UserStockAlertClient userStockAlertClient;

    public void notify(UserStockAlertDTO event, double price) {
        // Send to notification-service via Kafka
        log.info("ThreadName: {} notify {} for : {} has threshold value {} as triggerType {} for current price {}", Thread.currentThread().getName(),
        		event.getUserId(), event.getSymbol(), event.getThreshold(), event.getTriggerType().name(), price);
    }
    
    public void markAsTriggered(UserStockAlertDTO event, double price) {
        log.info("ThreadName: {} markAsTriggered {} for : {} has threshold value {} as triggerType {} for current price {}", Thread.currentThread().getName(),
        		event.getUserId(), event.getSymbol(), event.getThreshold(), event.getTriggerType().name(), price);
        userStockAlertClient.markAlertAsTriggered(event.getAlertId());
    }
}