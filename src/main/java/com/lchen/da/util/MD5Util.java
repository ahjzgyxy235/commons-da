package com.lchen.da.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

/** 
 * MD5工具类，对指定输入字符串进行计算并返回摘要值
 * 
 * @author hzchenlei1
 *
 * 2016年9月30日
 */
public class MD5Util {
	
	private static final Logger LOG = Logger.getLogger(MD5Util.class);
	
	/**
	 * 对原始字符串进行MD5处理
	 * @param originalStr
	 * @return 返回MD5摘要值，如果计算异常则返回null
	 */
	public static String getMD5(String originalStr){
		if(null==originalStr || "".equals(originalStr)){
			LOG.warn("Input original string couldn't be null, so return null now!");
			return null;
		}
		
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(originalStr.getBytes());
			return new BigInteger(1, md.digest()).toString(16);
		} catch (NoSuchAlgorithmException e) {
			LOG.error("Get md5 error and return null! Input value is:"+originalStr, e);
			return null;
		}
	}
	
	public static void main(String[] args) { System.out.println(MD5Util.getMD5("123456"));}
}




