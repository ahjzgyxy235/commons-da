package com.lchen.da.util.hbase;

import com.lchen.da.util.MD5Util;

/** 
 * HBase rowkey 生成策略<br>
 * 
 * 业务方根据Policy中的各种方法去组装实现各自需要的rowkey格式，也可以实现自己的policy
 * 
 * @author hzchenlei1
 *
 * 2016年9月30日
 */
public abstract class RowKeyPolicy {
	
	// 默认的MD5前缀截取长度, 从index=0开始截取
	private static final int DEFAULT_MD5_LENGTH = 4;
	
	// rowkey前缀后缀连接符
	String ROWKEY_CONNECT_SYMBOL = "_";
	
	/**
	 * 对原始数据进行散列操作
	 * @param originalPrefix
	 * @return 返回散列后的rowkey前缀,默认取前四位字符
	 */
	public String hashPrefix(String originalPrefix){
		String md5 = MD5Util.getMD5(originalPrefix);
		if(null==md5){
			throw new RuntimeException("Get a null md5 result!");
		}
		return md5.substring(0, DEFAULT_MD5_LENGTH);
	}
	
	/**
	 * 对年月日格式的时间进行反转操作，具体采用 (100000000-yyyyMMdd)的方式保证时间倒序排列
	 * @param yyyyMMdd 8位年月日格式日期字符串
	 * @return 100000000-yyyyMMdd
	 */
	public String reverseTime(String yyyyMMdd){
		if(null==yyyyMMdd || "".equals(yyyyMMdd)){
			throw new IllegalArgumentException("Time couldn't by null or empty!");
		}
		
		if(yyyyMMdd.length()!=8){
			throw new IllegalArgumentException(yyyyMMdd+" isn't right format(yyyyMMdd)!");
		}
		
		// TODO 这里对yyyyMMdd格式日期数字的校验写的不严谨 ...  
		int timeNumber = 0;
		try {
			timeNumber = Integer.valueOf(yyyyMMdd).intValue();
		} catch (Exception e) {
			throw new IllegalArgumentException(yyyyMMdd + " isn't number format!", e);
		}
		
		// 做时间倒序计算
		int minuend = 100000000;
		
		return String.valueOf(minuend-timeNumber);
	}
	
	/**
	 * 自定义rowkey组装规则，将输入元素组装变换成业务方特定需求的rowkey样式
	 * @param inputElements 可以支持多个字符型类型元素
	 * @return 组装好的rowkey
	 */
	public abstract String assembleRowKey(String... inputElements);
	
}




