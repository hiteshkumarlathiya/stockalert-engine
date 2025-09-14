package com.stockalert.api.client;

import java.util.List;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.stockalert.common.UserStockAlertDTO;

@FeignClient(name = "alert-api-service", url = "${alert.api.service.url}")
public interface UserStockAlertClient {

    @GetMapping("/api/alerts/symbol/{symbol}")
    List<UserStockAlertDTO> getAlertsBySymbol(@PathVariable("symbol") String symbol);

    @GetMapping("/api/alerts/symbol/{symbol}/range")
    List<UserStockAlertDTO> getAlertsBySymbolAndThresholdRange(@PathVariable("symbol") String symbol,
                                                             @RequestParam("min") double min,
                                                             @RequestParam("max") double max);
    @GetMapping("/api/alerts/symbols")
    List<UserStockAlertDTO> getAlertsBySymbols(@RequestParam("symbols") List<String> list);
    
    @PutMapping("/api/alerts/{id}/triggered")
    void markAlertAsTriggered(@PathVariable("id") Long id);
}