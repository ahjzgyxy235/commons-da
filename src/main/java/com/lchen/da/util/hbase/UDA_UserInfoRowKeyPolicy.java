package com.lchen.da.util.hbase;

import org.apache.log4j.Logger;

/** 
 * 互联网产品分析系统 - 用户信息HBase存储表rowkey设计规则
 * 
 * @author hzchenlei1
 *
 * 2016年10月9日
 */
public class UDA_UserInfoRowKeyPolicy extends RowKeyPolicy {
	
	private static final Logger LOG = Logger.getLogger(UDA_UserInfoRowKeyPolicy.class);
	
	/**
	  * 用户信息表rowkey规则如下：<br>
	 *    MD5(userId)_productKey_userId<br>
	 *    4f61_LOFTER_abcdefg@163.com<br>
	 * @param inputElements 两个输入元素，规定顺序<br>
	 * 	  0 - productKey<br>
	 * 	  1 - userId<br>
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
		
		StringBuffer sb = new StringBuffer();
		sb.append(rk_prefix).append(ROWKEY_CONNECT_SYMBOL).append(productKey).append(ROWKEY_CONNECT_SYMBOL)
		  .append(userId);
		
		return sb.toString();
	}
	
	/**
	 * 对输入元素进行合法性校验
	 * @param inputElements
	 * @return true or false 
	 */
	private boolean checkArgs(String... inputElements){
		
		boolean checkResult = false;
		
		if(null==inputElements || inputElements.length<2){
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
		
		LOG.debug("Assemble user info table rowkey: productKey["+productKey+"], userId["+userId+"]");
		
		checkResult = true;
		return checkResult;
	}
	
	public static void main(String[] args) {
		if(args.length<2){
			System.out.println("Usage: userInfoRowkey productId userId");
			return;
		}
		UDA_UserInfoRowKeyPolicy p = new UDA_UserInfoRowKeyPolicy();
		System.out.println(p.assembleRowKey(args[0], args[1]));
	}
	
}




