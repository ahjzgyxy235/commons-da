package com.lchen.da.cache;

import java.io.Serializable;
import java.util.Properties;


/** 
 * 缓存结果
 * @author hzchenlei1
 *
 * 2017-1-4
 */
public class CacheResult implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String cacheID;          // 全局唯一的缓存ID
	private QuerySource querySource; // 缓存数据对应的查询源
	private String cacheValue;       // 缓存数据, 以字符串表示
	private long cacheTime;          // 缓存数据插入时间
	@Deprecated
	private long dataValidTime;	     // 缓存数据有效期 - 截止日期
	// 查询参数
	private Properties queryParams = new Properties();
	private int ttlTime = -1;	 	 // 缓存数据剩余存活时间 单位秒
	
	/**
	 * @param cacheID 唯一缓存ID
	 * @param querySource 查询来源
	 * @param cacheValue 缓存数据值 
	 * @param dataValidTime 缓存数据有效时间
	 */
	public CacheResult(String cacheID, QuerySource querySource,
			String cacheValue, long dataValidTime) {
		if(null==cacheID || "".equals(cacheID)){
			throw new IllegalArgumentException("CacheID must not be null");
		}
		this.cacheID = cacheID;
		this.querySource = querySource;
		this.cacheValue = cacheValue;
		this.dataValidTime = dataValidTime;
		this.cacheTime = System.currentTimeMillis(); // 取当前服务器时间
	}
	
	/**
	 * @param cacheID 唯一缓存ID
	 */
	public CacheResult(String cacheID) {
		this(cacheID, QuerySource.others, null, -1L);
	}
	
	/**
	 * @param cacheID 唯一缓存ID
	 * @param cacheValue 缓存数据值 
	 */
	public CacheResult(String cacheID, String cacheValue) {
		this(cacheID, QuerySource.others, cacheValue, -1L);
	}
	
	/**
	 * @param cacheID 唯一缓存ID
	 * @param cacheValue 缓存数据值 
	 * @param dataValidTime 缓存数据有效时间
	 */
	public CacheResult(String cacheID, String cacheValue, long dataValidTime) {
	 	this(cacheID, QuerySource.others, cacheValue, dataValidTime);
	}
	
	/**
	 * @param cacheID 唯一缓存ID
	 * @param cacheValue 缓存数据值 
	 * @param querySource 查询来源
	 */
	public CacheResult(String cacheID, String cacheValue, QuerySource querySource) {
		this(cacheID, QuerySource.others, cacheValue, -1L);
	}
	
	/**
	 * 获取查询配置项集合</p>
	 * 客户端程序可以在这个配置集合中设置各种查询参数, 例如：<br>
	 * query.timerange.startday - 2017-01-01<br>
	 * query.timerange.endday - 2017-01-10<br>
	 * ... <br>
	 * @return queryParams 查询参数配置
	 */
	public Properties getQueryParams(){
		return this.queryParams;
	}
	
	public String getCacheID() {
		return cacheID;
	}
	public QuerySource getQuerySource() {
		return querySource;
	}
	public void setQuerySource(QuerySource querySource) {
		this.querySource = querySource;
	}
	public String getCacheValue() {
		return cacheValue;
	}
	public void setCacheValue(String cacheValue) {
		this.cacheValue = cacheValue;
	}
	public long getCacheTime() {
		return cacheTime;
	}
	public long getDataValidTime() {
		return dataValidTime;
	}
	public void setDataValidTime(long dataValidTime) {
		this.dataValidTime = dataValidTime;
	}
	public int getTtlTime() {
		return ttlTime;
	}
	protected void setTtlTime(int ttlTime) {
		this.ttlTime = ttlTime;
	}
}


