package com.lchen.da.udf;

import org.junit.Assert;
import org.junit.Test;

/** 
 * @author hzchenlei1
 *
 * 2017-12-4
 */
public class TimeWindowTest {
	
	/**
	 * 无效timestamp，返回null
	 */
	@Test
	public void testEvaluate1() {
		TimeWindow tw = new TimeWindow();
		String result = tw.evaluate(-1, 10);
		Assert.assertNull(result);
	}
	
	/**
	 * 不合法时间窗口，返回null
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testEvaluate2() {
		TimeWindow tw = new TimeWindow();
		tw.evaluate(1512374655000L, 8);
	}
	
	/**
	 * 合法参数, 窗口参数是1，直接返回格式化后的窗口归档
	 */
	@Test
	public void testEvaluate3() {
		TimeWindow tw = new TimeWindow();
		String reslut = tw.evaluate(1512374655000L, 1);
		Assert.assertEquals("201712041604-1", reslut);
	}
	
	/**
	 * 合法参数, 窗口参数是5，返回格式化后的窗口归档
	 */
	@Test
	public void testEvaluate4() {
		TimeWindow tw = new TimeWindow();
		String reslut = tw.evaluate(1512374655000L, 5);
		Assert.assertEquals("201712041600-5", reslut);
	}
	
	/**
	 * 合法参数, 窗口参数是30，返回格式化后的窗口归档
	 */
	@Test
	public void testEvaluate5() {
		TimeWindow tw = new TimeWindow();
		String reslut = tw.evaluate(1512374655000L, 30);
		Assert.assertEquals("201712041600-30", reslut);
	}
	
	/**
	 * 合法参数, 窗口参数是30，返回格式化后的窗口归档
	 */
	@Test
	public void testEvaluate6() {
		TimeWindow tw = new TimeWindow();
		String reslut = tw.evaluate(1512376335000L, 30);
		Assert.assertEquals("201712041630-30", reslut);
	}
	
	/**
	 * 合法参数, 窗口参数是60，返回格式化后的窗口归档
	 */
	@Test
	public void testEvaluate7() {
		TimeWindow tw = new TimeWindow();
		String reslut = tw.evaluate(1512377775000L, 60);
		Assert.assertEquals("201712041655-60", reslut);
	}
}




