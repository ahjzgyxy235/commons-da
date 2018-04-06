package com.lchen.da.util;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * 报警工具类
 */
public class AlermUtil {
	
	private static Logger logger = LoggerFactory.getLogger(AlermUtil.class);
	
	public static enum AlermType {
		// 短信报警
		Sms,
		// 易信报警
		Yixin,
		// 泡泡报警
		Popo,
		// 语音报警
		Voice;

		public static AlermType getByAlermName(String alermName) {
			for (AlermType alermType : AlermType.values()) {
				if (alermType.name().equalsIgnoreCase(alermName))
					return alermType;
			}
			logger.warn("AlermType name not defined. name = " + alermName);
			return null;
		}
	}

	public static void alerm(String body, String toList, AlermType alermType) {
		alerm(body, toList, Lists.newArrayList(alermType));
	}

	public static void alerm(String body, String toList, List<AlermType> alermTypes) {
		for (AlermType alermType : alermTypes) {
			try {

				switch (alermType) {
				case Sms:
					sendAlerm(body, toList, "http://alarm.netease.com/api/sendSMS", alermType.name());
					break;
				case Yixin:
					sendAlerm(body, toList, "http://alarm.netease.com/api/sendYiXin", alermType.name());
					break;
				case Popo:
					sendAlerm(body, toList, "http://alarm.netease.com/api/sendPOPO", alermType.name());
					break;
				case Voice:
					sendAlerm(body, toList, "http://alarm.netease.com/api/sendVoice", alermType.name());
					break;
				default:
					break;
				}
			} catch (Exception e) {
				logger.error("send alerm exception", e);
			}
		}
	}

	private static void sendAlerm(String body, String toList, String url, String alermType) throws Exception {
		String appName = "da";
		String secret = "858f7563-6d18-43b3-80ed-1ed45476bed2";
		long timestamp = new Date().getTime();
		String signature = generateSignature(secret, timestamp);

		List<String> params = Lists.newArrayList();
		params.add("ac_appName=" + appName);
		params.add("ac_timestamp=" + timestamp);
		params.add("ac_signature=" + signature);
		params.add("content=" + URLEncoder.encode(body, "utf-8"));
		params.add("to=" + toList);
		params.add("isSync=1");
		String parameter = Joiner.on("&").join(params);

		url = url + "?" + parameter;
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setDoInput(true);
		connection.setDoOutput(false);
		connection.setRequestMethod("GET");

		if (connection.getResponseCode() != 200)
			throw new RuntimeException("send msg failed with code: " + connection.getResponseCode());
	}

	private static String generateSignature(String secret, long timestamp) throws NoSuchAlgorithmException {
		String value = secret + timestamp;

		MessageDigest messageDigest = MessageDigest.getInstance("md5");
		messageDigest.update(value.getBytes());
		byte[] bb = messageDigest.digest();

		String s = getFormattedText(bb);
		return s;
	}

	private static String getFormattedText(byte[] bytes) {
		int len = bytes.length;
		StringBuilder buf = new StringBuilder(len * 2);
		for (int j = 0; j < len; j++) {
			buf.append(HEX_DIGITS[(bytes[j] >> 4) & 0x0f]);
			buf.append(HEX_DIGITS[bytes[j] & 0x0f]);
		}
		return buf.toString();
	}

	private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	public static void main(String[] args) throws Exception {
		String alertList = "hzchenlei1@corp.netease.com,hzhuyifan@corp.netease.com";
		AlermUtil.alerm("test", alertList, AlermType.Popo);
	}
}
