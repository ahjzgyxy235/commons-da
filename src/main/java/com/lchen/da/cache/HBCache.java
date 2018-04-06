package com.lchen.da.cache;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/** 
 * Hubble cache
 * @author hzchenlei1
 *
 * 2017-1-4
 */
public abstract class HBCache {
	
	private static final Logger LOG = Logger.getLogger(HBCache.class);
	private final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd");
	// cache时长 = 基本单位 一小时
	private static final int CACHE_AN_HOUR = 60 * 60;
	
	public abstract void insert(String cacheID, CacheResult data);
	
	public abstract void update(String cacheID, CacheResult data);
	
	public abstract CacheResult query(String cacheID);
	
	public abstract String createCacheID(String inputStr, QuerySource qs);
	
	/**
	 * 根据缓存数据中的相关查询参数计算该条记录的过期时间
	 * @param cacheResult
	 * @return = -1  永久缓存<br>
	 * 		   >  0  指定缓存时间(秒)<br>
	 */
	public int getExpireTime(CacheResult cacheResult){
		
		Calendar calendar = Calendar.getInstance();
		
		// 默认结果
		int defaultResult = -1;
		
		// sql查询日期边界
		String queryStartDay = cacheResult.getQueryParams().getProperty(QueryOptions.QUERY_TIMERANAGE_START_DAY);
		LOG.debug(QueryOptions.QUERY_TIMERANAGE_START_DAY+": "+queryStartDay);
		
		String queryEndDay   = cacheResult.getQueryParams().getProperty(QueryOptions.QUERY_TIMERANAGE_END_DAY);
		LOG.debug(QueryOptions.QUERY_TIMERANAGE_END_DAY+": "+queryEndDay);
		
		// 1.如果未设置查询日期边界，则直接将查询结果全部缓存
		if(StringUtils.isBlank(queryStartDay) || StringUtils.isBlank(queryEndDay)){
			LOG.warn("No query time boundary was found in query parameters and cache the result of that query forever!");
			return defaultResult;
		}
		
		// 校验日期格式
		try {
			Date queryStartDate = SDF.parse(queryStartDay);
			Date queryEndDate   = SDF.parse(queryEndDay);
			if(queryStartDate.after(queryEndDate)){
				LOG.warn("query start day after than end day!");
				return defaultResult;
			}
		} catch (ParseException e) {
			LOG.warn("SimpleDateFormat parse date failed!", e);
			return defaultResult;
		}
		
		// 当前天
		String currentDay = SDF.format(calendar.getTime());
		
		// 7天前
		calendar.add(Calendar.DAY_OF_MONTH, -8);
		String lastDay = SDF.format(calendar.getTime());
		
		// 2.如果查询截止日期大于等于当前天，则保存一小时
		if(queryEndDay.compareTo(currentDay) >= 0){
			return 1 * CACHE_AN_HOUR;
		}
		
		// 3.如果查询截止日期小于lastDay，则缓存时长两周
		if(queryEndDay.compareTo(lastDay) < 0){
			return 14 * 24 * CACHE_AN_HOUR;
		}
		
		// 4.如果查询时间区间在{lastDay ~ currentDay}内，则缓存一天
		if(queryEndDay.compareTo(currentDay) < 0 && queryEndDay.compareTo(lastDay) >= 0){
			return 1 * 24 * CACHE_AN_HOUR;
		}
		
		return defaultResult;
	}
	
	public static void main(String[] args) {
		CacheResult data = new CacheResult("123", "cacheValue");
		// 设置查询相关参数，这里主要设置了query sql中的最大范围时间边界
		data.getQueryParams().setProperty(QueryOptions.QUERY_TIMERANAGE_START_DAY, "2017-01-16");
		data.getQueryParams().setProperty(QueryOptions.QUERY_TIMERANAGE_END_DAY, "2017-01-22");
		//System.out.println(HBCache.getExpireTime(data));
	}
	
}


