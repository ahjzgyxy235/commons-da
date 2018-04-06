package com.lchen.da.util.hbase;

import org.junit.Assert;
import org.junit.Test;


/** 
 * @author hzchenlei1
 *
 * 2016年10月10日
 */
public class UDA_UserEventsRowKeyPolicyTest {
	
	RowKeyPolicy rkp = new UDA_UserEventsRowKeyPolicy();
	
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
	 * 非法时间格式输入
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testReverseTime1() {
		rkp.reverseTime("");
	}
	
	/**
	 * 非法时间格式输入
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testReverseTime2() {
		rkp.reverseTime(null);
	}
	
	/**
	 * 非法时间格式输入
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testReverseTime3() {
		rkp.reverseTime("123456789");
	}
	
	/**
	 * 非法时间格式输入
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testReverseTime4() {
		rkp.reverseTime("1234567A");
	}
	
	/**
	 * 合法时间格式(yyyyMMdd)输入
	 */
	@Test
	public void testReverseTime5() {
		String reverseTime = rkp.reverseTime("20160212");
		Assert.assertEquals("79839788", reverseTime);
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
	 * 输入组装rowkey的元素不合法, 参数个数不对
	 */
	@Test
	public void testAssembleRowKey2(){
		String rk = rkp.assembleRowKey("LOFTER");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法, 参数个数不对
	 */
	@Test
	public void testAssembleRowKey3(){
		String rk = rkp.assembleRowKey("LOFTER","abcdefg@163.com");
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
		String rk = rkp.assembleRowKey("", "");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法
	 */
	@Test
	public void testAssembleRowKey6(){
		String rk = rkp.assembleRowKey("", "", "");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法, 时间参数不合法
	 */
	@Test
	public void testAssembleRowKey7(){
		String rk = rkp.assembleRowKey("LOFTER","abcdefg@163.com", null);
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法, 时间参数不合法
	 */
	@Test
	public void testAssembleRowKey8(){
		String rk = rkp.assembleRowKey("LOFTER","abcdefg@163.com", "");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素不合法, 时间参数不合法
	 */
	@Test
	public void testAssembleRowKey9(){
		String rk = rkp.assembleRowKey("LOFTER","abcdefg@163.com", "201602122");
		Assert.assertNull(rk);
	}
	
	/**
	 * 输入组装rowkey的元素合法, 正确输出结果
	 */
	@Test
	public void testAssembleRowKey10(){
		String rk = rkp.assembleRowKey("LOFTER","abcdefg@163.com", "20160212");
		Assert.assertEquals("4f61_LOFTER_abcdefg@163.com_79839788", rk);
	}
	
}




