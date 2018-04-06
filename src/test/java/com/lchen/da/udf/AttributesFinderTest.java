package com.lchen.da.udf;

import org.junit.Assert;
import org.junit.Test;

/** 
 * @author hzchenlei1
 *
 * 2017-12-4
 */
public class AttributesFinderTest {
	
	private SlothAttributesFinder af = new SlothAttributesFinder();
	
	/**
	 * 按照jsonKey正确取出jsonValue
	 */
	@Test
	public void testEvaluate1(){
		String attributesStr = "{\"$screenName\":\"LFTTagViewController\",\"$screenTitle\":\"正义联盟\"}";
		String jsonValue = af.evaluate(attributesStr, "$screenTitle");
		Assert.assertEquals("正义联盟", jsonValue);
	}
	
	/**
	 * 异常attributes值，解析失败，返回null
	 */
	@Test
	public void testEvaluate2(){
		String attributesStr = "{\"$screenName\":LFTTagViewController\",\"$screenTitle\":\"正义联盟\"}";
		String jsonValue = af.evaluate(attributesStr, "$screenTitle");
		Assert.assertEquals(null, jsonValue);
	}
	
	/**
	 * 空的attributes，解析失败，返回null
	 */
	@Test
	public void testEvaluate3(){
		String attributesStr = "";
		String jsonValue = af.evaluate(attributesStr, "$screenTitle");
		Assert.assertEquals(null, jsonValue);
	}
	
	/**
	 * 空的jsonKey，返回null
	 */
	@Test
	public void testEvaluate4(){
		String attributesStr = "{\"$screenName\":\"LFTTagViewController\",\"$screenTitle\":\"正义联盟\"}";
		String jsonValue = af.evaluate(attributesStr, null);
		Assert.assertEquals(null, jsonValue);
	}
	
	/**
	 * 不包含jsonKey的attributes，搜索key失败，返回null
	 */
	@Test
	public void testEvaluate5(){
		String attributesStr = "{}";
		String jsonValue = af.evaluate(attributesStr, "$screenTitle");
		Assert.assertEquals(null, jsonValue);
	}
}




