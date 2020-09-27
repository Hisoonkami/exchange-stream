package com.adev.exchange.stream.service.impl;

import com.adev.common.base.domian.BaseResult;
import com.adev.common.base.enums.ResultEnum;
import com.adev.exchange.stream.service.GatherFeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;


@Service
public class GatherFeignClientImpl implements GatherFeignClient {

	@Override
	public ResponseEntity<BaseResult> findCurrencyPairByExchange(String exchange) {
		return ResponseEntity.ok(BaseResult.failure(ResultEnum.INTERFACE_EXCEED_LOAD));
	}
}
