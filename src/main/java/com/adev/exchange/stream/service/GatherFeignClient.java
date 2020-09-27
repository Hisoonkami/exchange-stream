package com.adev.exchange.stream.service;

import com.adev.common.base.domian.BaseResult;
import com.adev.exchange.stream.service.impl.GatherFeignClientImpl;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@FeignClient(name = "gather",fallback = GatherFeignClientImpl.class)
public interface GatherFeignClient {

	@RequestMapping(value = {"/api/currencyPairs/search/findByExchange"},method = RequestMethod.GET)
	@ResponseBody
	ResponseEntity<BaseResult> findCurrencyPairByExchange(@RequestParam(value = "exchange") String exchange);
	
}
