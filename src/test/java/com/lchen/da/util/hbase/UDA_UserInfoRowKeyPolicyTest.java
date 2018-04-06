package com.lchen.da.util.hbase;

import org.junit.Assert;
import org.junit.Test;

/** 
 * @author hzchenlei1
 *
 * 2016年10月10日
 */
public class UDA_UserInfoRowKeyPolicyTest {
	
	RowKeyPolicy rkp = new UDA_UserInfoRowKeyPolicy();

	/**
	 * 无效输入值，抛出异常
	 */
	@Test(expected=RuntimeException.class)
	public void testHashPrefix1() {
		rkp.hashPrefix("");
	}
	
	/**
	 * 无效输入值，抛出异常
	 */
	@Test(expected=RuntimeException.class)
	public void testHashPrefix2() {
		rkp.hashPrefix(null);
	}
	
	/**
	 * 正确输入值
	 */
	@Test
	public void testHashPrefix3() {
		String hashPrefix = rkp.hashPrefix("123456789");
		Assert.assertEquals("25f9", hashPrefix);
	}
	
	/**
	 * 输入组装rowkey的元素不合法
	 */
	@Test
	public void testAssembleRowKey1(){
		String rk = rkp.assembleRowKey(null);
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法
	 */
	@Test
	public void testAssembleRowKey4(){
		String rk = rkp.assembleRowKey(null, null);
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法
	 */
	@Test
	public void testAssembleRowKey5(){
		String rk = rkp.assembleRowKey("");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法
	 */
	@Test
	public void testAssembleRowKey6(){
		String rk = rkp.assembleRowKey("", "");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法, 参数个数不对
	 */
	@Test
	public void testAssembleRowKey2(){
		String rk = rkp.assembleRowKey("LOFTER");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素合法，输出正确结果
	 */
	@Test
	public void testAssembleRowKey3(){
		String rk = rkp.assembleRowKey("LOFTER","abcdefg@163.com");
		Assert.assertEquals("4f61_LOFTER_abcdefg@163.com", rk);
	}
	
	

}




