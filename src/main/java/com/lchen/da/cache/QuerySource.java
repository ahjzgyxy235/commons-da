package com.lchen.da.cache;

/** 
 * 此处枚举定义了所有的缓存数据查询来源
 * @author hzchenlei1
 *
 * 2017-1-12
 */
public enum QuerySource {
	segmentation,  // 事件分析
	funnels,       // 漏斗分析
	retention,     // 留存分析
	addiction,     // 粘性分析
	live,          // 实时分析
	others;        // 其他 
}




