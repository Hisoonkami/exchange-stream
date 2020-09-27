package com.adev.exchange.stream.exchange.bitbank;

import com.adev.common.base.utils.DataUtils;
import com.adev.common.exchange.webscoket.JsonStreamingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xn 2018/10/27 9:10
 */
public class BitbankStreamingService extends JsonStreamingService {

	private static final Logger LOG = LoggerFactory.getLogger(BitbankStreamingService.class);

	public BitbankStreamingService(String apiUrl) {
		super(apiUrl, Integer.MAX_VALUE, Duration.ofSeconds(10), Duration.ofSeconds(30));
	}
	@Override
	public String exName() {
		return "Bitbank";
	}

	@Override
	protected WebSocketClientExtensionHandler getWebSocketClientExtensionHandler() {
		return null;
	}

	@Override
	public void messageHandler(String message) {

		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode;

		try {

			message = message.replace(" ", "");

			if(message.startsWith("42[\"message\",")){

				message = message.replace("42[\"message\",", "");
				message = message.substring(0, message.length() - 1);
				jsonNode = objectMapper.readTree(message);
				handleMessage(jsonNode);
			}

		} catch (IOException e) {
			LOG.error("Error parsing incoming message to JSON: {}", message);
			return;
		}

	}

	@Override
	protected void handleMessage(JsonNode message) {

		if("40".equals(DataUtils.objToStr(message))){
			LOG.debug("订阅成功,{}", DataUtils.objToStr(message));
		}else{

			super.handleMessage(message);
		}
	}

	@Override
	protected String getChannelNameFromMessage(JsonNode message) throws IOException {

		String chanId = null;
		if(message.has("room_name")){

			chanId = DataUtils.objToStr(message.get("room_name"));
		}
		return chanId;
	}

	@Override
	public String getSubscriptionUniqueId(String channelName, Object... args) {
		return channelName;
	}

	@Override
	public String getSubscribeMessage(String channelName, Object... args) throws IOException {

		String subStr = "42[\"join-room\",\"" + channelName + "\"]";
		return subStr;
	}

	@Override
	public String getUnsubscribeMessage(String channelName) throws IOException {
		return null;
	}
	@Override
	public boolean ping() {

		sendMessage("2");
		return true;
	}
}
