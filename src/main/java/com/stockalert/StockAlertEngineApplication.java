package com.stockalert;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication(exclude = { org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration.class })
@EnableFeignClients(basePackages = "com.stockalert.api.client")
@EnableAsync
public class StockAlertEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(StockAlertEngineApplication.class, args);
	}

}
