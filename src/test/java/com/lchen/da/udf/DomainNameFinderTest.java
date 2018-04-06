package com.lchen.da.udf;

import org.junit.Assert;
import org.junit.Test;

/** 
 * @author hzchenlei1
 *
 * 2017-12-19
 */
public class DomainNameFinderTest {

	@Test
	public void testEvaluate1() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertNull(df.evaluate(""));
	}
	
	@Test
	public void testEvaluate2() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertNull(df.evaluate(null));
	}
	
	/**
	 * 搜索
	 */
	@Test
	public void testEvaluate4() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("百度", df.evaluate("www.baidu.com"));
	}
	
	/**
	 * 搜索
	 */
	@Test
	public void testEvaluate5() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("谷歌", df.evaluate("www.google.com.hk"));
	}
	
	/**
	 * 搜索
	 */
	@Test
	public void testEvaluate13() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("谷歌", df.evaluate("www.google.com"));
	}
	
	/**
	 * 社交
	 */
	@Test
	public void testEvaluate6() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("百度Hi", df.evaluate("hi.baidu.com"));
	}
	
	/**
	 * 社交
	 */
	@Test
	public void testEvaluate7() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("新浪微博", df.evaluate("weibo.com"));
	}
	
	/**
	 * 直播
	 */
	@Test
	public void testEvaluate8() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("bilibili", df.evaluate("live.bilibili.com"));
	}
	
	/**
	 * 电商
	 */
	@Test
	public void testEvaluate9() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("京东", df.evaluate("jd.com"));
	}
	
	/**
	 * 视频
	 */
	@Test
	public void testEvaluate10() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("好看视频", df.evaluate("haokan.baidu.com"));
	}
	
	/**
	 * 新闻
	 */
	@Test
	public void testEvaluate11() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("百度新闻", df.evaluate("news.baidu.com"));
	}
	
	/**
	 * 其他链接
	 */
	@Test
	public void testEvaluate12() {
		DomainNameFinder df = new DomainNameFinder();
		Assert.assertEquals("hubble.netease.com", df.evaluate("hubble.netease.com"));
	}
}




