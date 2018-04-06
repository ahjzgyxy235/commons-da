package com.lchen.da.udf;

import org.junit.Assert;
import org.junit.Test;

/** 
 * @author hzchenlei1
 *
 * 2017-12-19
 */
public class DomainClassifierTest {
	
	// 直接进入
	@Test
	public void testEvaluate1() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("directEntry", dc.evaluate(""));
	}
	
	// 直接进入
	@Test
	public void testEvaluate2() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("directEntry", dc.evaluate(null));
	}
	
	// 直接进入
	@Test
	public void testEvaluate3() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("directEntry", dc.evaluate(" "));
	}
	
	/**
	 * 搜索
	 */
	@Test
	public void testEvaluate4() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("search", dc.evaluate("www.baidu.com"));
	}
	
	/**
	 * 搜索
	 */
	@Test
	public void testEvaluate5() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("search", dc.evaluate("www.google.com.hk"));
	}
	
	/**
	 * 社交
	 */
	@Test
	public void testEvaluate6() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("social", dc.evaluate("hi.baidu.com"));
	}
	
	/**
	 * 社交
	 */
	@Test
	public void testEvaluate7() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("social", dc.evaluate("weibo.com"));
	}
	
	/**
	 * 直播
	 */
	@Test
	public void testEvaluate8() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("live", dc.evaluate("live.bilibili.com"));
	}
	
	/**
	 * 电商
	 */
	@Test
	public void testEvaluate9() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("shop", dc.evaluate("jd.com"));
	}
	
	/**
	 * 视频
	 */
	@Test
	public void testEvaluate10() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("video", dc.evaluate("haokan.baidu.com"));
	}
	
	/**
	 * 新闻
	 */
	@Test
	public void testEvaluate11() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("news", dc.evaluate("news.baidu.com"));
	}
	
	/**
	 * 其他链接
	 */
	@Test
	public void testEvaluate12() {
		DomainClassifier dc = new DomainClassifier();
		Assert.assertEquals("otherLink", dc.evaluate("hubble.netease.com"));
	}

}




