package com.lchen.da.util;

import org.junit.Assert;
import org.junit.Test;

/** 
 * @author hzchenlei1
 *
 * 2016年10月10日
 */
public class MD5UtilTest {
	
	/**
	 * 空输入 返回null
	 */
	@Test
	public void testGetMD5() {
		String actualResult1 = MD5Util.getMD5(null);
		Assert.assertNull(actualResult1);
		
		String actualResult2 = MD5Util.getMD5("");
		Assert.assertNull(actualResult2);
	}
	
	/**
	 * 正常输入数据，返回正确的32位MD5值
	 */
	@Test
	public void testGetMD52() {
		String actualResult = MD5Util.getMD5("123456789");
		System.out.println(actualResult);
		Assert.assertEquals("25f9e794323b453885f5181f1b624d0b", actualResult);
	}

}




