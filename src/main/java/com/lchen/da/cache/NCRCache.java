package com.lchen.da.cache;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.lchen.da.util.MD5Util;
import com.lchen.da.util.SerializeUtil;


/** 
 * 基于NCR的缓存操作实现
 * @author hzchenlei1
 *
 * 2017-1-4
 */
public class NCRCache extends HBCache {
	
	private static final Logger LOG = LoggerFactory.getLogger(NCRCache.class);
	// uda系统的缓存数据在NCR中的缓存前缀
	private static final String CACHEID_PREFIX = "uda%cacheid";
	// 默认最大ttl时间 = 30 days
	private static final int MAX_EXPIRE_TIME = 30 * 24 * 60 * 60;
	private static JedisPool jedisPool = null;
	
	/**
	 * NCR缓存构造器</p>
	 * @param ncrProps 配置信息，具体如下<br>
	 * ncr_online_host 必填项<br>
	 * ncr_online_port 必填项<br>
	 * ncr_online_passwd 选填项<br>
	 * @exception IllegalArgumentException 如果ncrProps为空或者参数不完整则会抛出异常
	 */
	public NCRCache(Properties ncrProps){
		if(null==ncrProps || ncrProps.size()<1){
			throw new IllegalArgumentException("Input property couldn't by empty/null!");
		}
		if(null==jedisPool){
			initJedisPool(ncrProps);
		}
	}
	
	/**
	 * 初始化jedis连接池
	 * @param p
	 * @exception IllegalArgumentException
	 */
	private synchronized void initJedisPool(Properties p){
		if(null==jedisPool){
			LOG.info("init ncr connection ...");
			// host
			String ncr_host = p.getProperty("ncr_online_host");
			if(null==ncr_host || "".equals(ncr_host)){
				throw new IllegalArgumentException("Invalid ncr_online_host!");
			}
			LOG.info("ncr.host: "+ncr_host);
			
			// port
			int ncr_port = Integer.parseInt(p.getProperty("ncr_online_port"));
			if(ncr_port<1024){
				throw new IllegalArgumentException("Invalid ncr_online_port: "+ncr_port);
			}
			LOG.info("ncr.port: "+p.getProperty("ncr_online_port"));
			
			// passwd
			String ncr_passwd = p.getProperty("ncr_online_passwd");
			LOG.info("ncr.passwd: "+p.getProperty("ncr_online_passwd"));
			
			// init pool
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxTotal(100); // pool最大连接数 default 8
			config.setMaxIdle(8);    // pool最大空闲连接数 default 8
			int timeout = 300000;    // 读取超时时间
			
			// 兼容本地测试环境，取消了对密码的强制非空判定
			if(null==ncr_passwd || "".equals(ncr_passwd)){
				jedisPool = new JedisPool(config, ncr_host, ncr_port, timeout);
			} else {
				jedisPool = new JedisPool(config, ncr_host, ncr_port, timeout, ncr_passwd);
			}
			
			LOG.info("init ncr connection pool successfully!");
		}
	}
	
	/**
	 * 获取jedis对象
	 * @return
	 */
	private Jedis getJedis(){
		return jedisPool.getResource();
	}
	
	/**
	 * 插入数据
	 * @exception RuntimeException
	 */
	public void insert(String cacheID, CacheResult data) {
		LOG.info("insert cacheID:"+cacheID);
		Jedis jedis = getJedis();
		try {
			int expireTime = super.getExpireTime(data);
			LOG.info("expire time for "+cacheID+": "+expireTime);
			if(expireTime > 0){
				// set expire time
				jedis.setex(cacheID.getBytes(), expireTime, SerializeUtil.serialize(data));
			}else{
				// max expire time
				jedis.setex(cacheID.getBytes(), MAX_EXPIRE_TIME, SerializeUtil.serialize(data));
			}
		} catch (Exception e) {
			LOG.error("jedis insert failed!");
			throw new RuntimeException(e);
		} finally {
			if(null!=jedis){
				jedis.close();
			}
		}
	}
	
	/**
	 * 更新数据
	 * @exception RuntimeException
	 */
	public void update(String cacheID, CacheResult data) {
		LOG.info("update cacheID:"+cacheID);
		Jedis jedis = getJedis();
		try {
			// delete old data
			jedis.del(cacheID.getBytes());
			// insert new data
			int expireTime = super.getExpireTime(data);
			LOG.info("expire time for "+cacheID+": "+expireTime);
			if(expireTime > 0){
				// set expire time
				jedis.setex(cacheID.getBytes(), expireTime, SerializeUtil.serialize(data));
			}else{
				// max expire time
				jedis.setex(cacheID.getBytes(), MAX_EXPIRE_TIME, SerializeUtil.serialize(data));
			}
		} catch (Exception e) {
			LOG.error("jedis update failed!");
			throw new RuntimeException(e);
		} finally {
			if(null!=jedis){
				jedis.close();
			}
		}
	}
	
	public CacheResult query(String cacheID) {
		LOG.info("query cacheID:"+cacheID);
		byte[] result = null;
		int ttlTime = -1;
		Jedis jedis = getJedis();
		try {
			result = jedis.get(cacheID.getBytes());
			ttlTime = Integer.parseInt(String.valueOf(jedis.ttl(cacheID.getBytes())));
		} catch (Exception e) {
			LOG.error("jedis query failed!");
			throw new RuntimeException(e);
		} finally {
			if(null!=jedis){
				jedis.close();
			}
		}
		
		if(null!=result){
			LOG.info("match cacheID:"+cacheID);
			CacheResult cacheResult = (CacheResult) SerializeUtil.unserialize(result);
			cacheResult.setTtlTime(ttlTime);
			return cacheResult;
		}
		return null;
	}
	
	/**
	 * 根据明文字符串生成随机的全局唯一的字符串<br>
	 * @param inputStr 查询明文sql语句
	 * @param querySource 查询来源，not null
	 * @return 全局唯一的字符串
	 * @exception IllegalArgumentException 如果异常参数，则会抛出无效参数异常
	 */
	public String createCacheID(String inputStr, QuerySource qs){
		String md5 = MD5Util.getMD5(inputStr);
		if(null==md5){
			LOG.error("create cacheID failed!");
			throw new IllegalArgumentException("invalid inputStr");
		}
		if(null==qs){
			LOG.error("create cacheID failed!");
			throw new IllegalArgumentException("querySource could not be null");
		}
		
		String cacheID = CACHEID_PREFIX.concat("_").concat(md5).concat("_").concat(qs.toString());
		LOG.info("created cacheID:"+cacheID);
		return cacheID;
	}

}

