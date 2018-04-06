package com.lchen.da.util.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

/** 
 * HBase 工具类<br><br>
 * 提供HBase htable对象构建方法
 * 
 * Usage: HBaseUtil.getHTable(HBaseConfiguration.create(), "HBase table name")
 * 
 * @author hzchenlei1
 *
 * 2016年10月12日
 */
public class HBaseUtil {
	
	private static final Logger LOG = Logger.getLogger(HBaseUtil.class);
	
	// Hconnection是线程安全的重对象，所以这里选择缓存共享
	private static HConnection connection = null;
	
	/**
	 * 获取hbase table对象<br>
	 * 注意: <br>
	 * 1.获取的HTableInterface对象是非线程安全的<br>
	 * 2.轻量级操作，不需要缓存或者池化获取到的HTableInterface对象<br>
	 * 3.HTableInterface对象对象使用完成后，调不调用<br>
	 *     htable.close()<br>
	 *   函数区别不大。如果调用，只会将连接对象的closed属性设置为true，不会去释放zookeeper连接(重量级对象);<br>
	 *   如果不调用，连接对象会由jvm自动回收;<br>
	 *   
	 * @param hbaseConf
	 * @param tableName
	 * @return HTableInterface
	 */
	public static synchronized HTableInterface getHTable(Configuration hbaseConf, String tableName){
		if(null==hbaseConf){
			throw new IllegalArgumentException("Couldn't create hbase table by null configuration!");
		}
		if(null==tableName || "".equals(tableName)){
			throw new IllegalArgumentException("Couldn't create hbase table by empty table name!");
		}
		// 缓存hconnection
		if(null==connection){
			try {
				connection = HConnectionManager.createConnection(hbaseConf);
			} catch (IOException e) {
				LOG.error("Appear IOException when create hbase connection!");
				throw new RuntimeException(e);
			}
		}
		
		LOG.debug("try to get hbase table: "+tableName);
		HTableInterface htable = null;
		try {
			htable = connection.getTable(tableName);
		} catch (IOException e) {
			LOG.error("Appear IOException when create hbase table!");
			throw new RuntimeException(e);
		}
		
		return htable;
	}
	
}




