package com.lchen.da.udf;

import com.netease.sloth.udf.UDF;

/** 
 * 随机double型数字生成工具
 * @author hzchenlei1
 *
 * 2017-12-5
 */
public class RandomDouble implements UDF{
	
	/**
	 * 直接使用Math类生成double型数字
	 * sloth提供的rand udf不可用，所以这里自己定义udf，同时命名时避免使用"rand" 
	 * @return
	 */
	public double evaluate(){
		return Math.random();
	}
}




