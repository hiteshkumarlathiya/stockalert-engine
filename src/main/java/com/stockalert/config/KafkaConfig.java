package com.stockalert.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.stockalert.common.StockPriceEvent;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

	@Value("${confluent.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${confluent.api-key}")
	private String apiKey;

	@Value("${confluent.api-secret}")
	private String apiSecret;

	private Map<String, Object> kafkaConfiguration() {
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		config.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required "
				+ "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";");
		//config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
		//config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-evaluator");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.stockalert.*");
		return config;
	}
	
	@Bean
    ConsumerFactory<String, StockPriceEvent> consumerFactory() {
		log.info("Inside consumerFactory");
//        return new DefaultKafkaConsumerFactory<>(kafkaConfiguration(), new StringDeserializer(), 
//        	      new JsonDeserializer<>(StockPriceEvent.class));
		return new DefaultKafkaConsumerFactory<>(kafkaConfiguration());
    }

	@Bean
    ConcurrentKafkaListenerContainerFactory<String, StockPriceEvent> kafkaListenerContainerFactory() {
		log.info("Inside kafkaListenerContainerFactory");
        ConcurrentKafkaListenerContainerFactory<String, StockPriceEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}