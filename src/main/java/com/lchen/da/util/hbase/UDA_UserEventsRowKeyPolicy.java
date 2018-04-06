package com.lchen.da.util.hbase;

import org.apache.log4j.Logger;

/** 
 * 互联网产品分析系统 - 用户事件流HBase存储表rowkey设计规则
 * 
 * @author hzchenlei1
 *
 * 2016年10月9日
 */
public class UDA_UserEventsRowKeyPolicy extends RowKeyPolicy {
	
	private static final Logger LOG = Logger.getLogger(UDA_UserEventsRowKeyPolicy.class);
	
	/**
	 * 用户事件流表rowkey规则如下：<br>
	 *    MD5(userId)_productKey_userId_yyyyMMdd<br>
	 *    4f61_LOFTER_abcdefg@163.com_79839078<br>
	 * @param inputElements 三个输入元素，规定顺序<br>
	 * 	  0 - productKey<br>
	 * 	  1 - userId<br>
	 * 	  2 - date[yyyyMMdd]<br>
	 */
	@Override
	public String assembleRowKey(String... inputElements) {
		if(!checkArgs(inputElements)){
			return null;
		}
		
		String productKey = inputElements[0];
		String userId = inputElements[1];
		
		// 对userId进行散列
		String rk_prefix = hashPrefix(userId);
		
		// 对时间进行倒序
		String reverseTime = reverseTime(inputElements[2]);
		
		StringBuffer sb = new StringBuffer();
		sb.append(rk_prefix).append(ROWKEY_CONNECT_SYMBOL).append(productKey).append(ROWKEY_CONNECT_SYMBOL)
		  .append(userId).append(ROWKEY_CONNECT_SYMBOL).append(reverseTime);
		
		return sb.toString();
	}
	
	/**
	 * 对输入元素进行合法性校验
	 * @param inputElements
	 * @return true or false 
	 */
	private boolean checkArgs(String... inputElements){
		
		boolean checkResult = false;
		
		if(null==inputElements || inputElements.length<3){
			LOG.warn("Input elements are illegal!");
			return checkResult;
		}
		// 产品标示
		String productKey = inputElements[0];
		if(null==productKey || "".equals(productKey)){
			LOG.warn("productKey coudn't be empty or null!");
			return checkResult;
		}
		// 用户ID
		String userId = inputElements[1];
		if(null==userId || "".equals(userId)){
			LOG.warn("userId coudn't be empty or null!");
			return checkResult;
		}
		// yyyyMMdd格式时间
		String date = inputElements[2];
		if(null==date){
			LOG.warn("date couldn't be null");
			return checkResult;
		}else if(date.length()!=8){
			LOG.warn("date argument:"+date+" is illegal!");
			return checkResult;
		}
		
		LOG.debug("Assemble user events table rowkey: productKey["+productKey+"], userId["+userId+"], date["+date+"]");
		
		checkResult = true;
		return checkResult;
	}
	
	public static void main(String[] args) {
		if(args.length<3){
			System.out.println("Usage: userEventRowkey productId userId date[yyyyMMdd]");
			return;
		}
		UDA_UserEventsRowKeyPolicy p = new UDA_UserEventsRowKeyPolicy();
		System.out.println(p.assembleRowKey(args[0], args[1], args[2]));
	}
	
}




