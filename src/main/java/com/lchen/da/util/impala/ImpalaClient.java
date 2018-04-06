package com.lchen.da.util.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

/** 
 * impala client<br>
 * 通过jdbc的方式连接impala集群进行操作<br><br>
 *  
 *  // 使用demo<br>
 *  
 *  // kerberos auth if need<br>
 *  Configuration kconf = new Configuration();<br>
 *	kconf.setBoolean("hadoop.security.authorization", true);<br>
 *	kconf.set("hadoop.security.authentication", "kerberos");<br>
 *	KerberosAuthentication.authenticate(kconf, "da@HADOOP.HZ.NETEASE.COM", "E:\\Kerberos\\da\\da.keytab");<br>
 *  
 *  // execute refresh sql<br>
 *	String hsUrl = "jdbc:hive2://hadoop460.lt.163.org:2181,hadoop461.lt.163.org:2181,hadoop462.lt.163.org:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hz-impala1-ha;principal=impala/_HOST@HADOOP.HZ.NETEASE.COM";<br>
 *	ImpalaClient client = new ImpalaClient(hsUrl);<br>
 *	client.executeDDL("refresh uda.uda_events Partition(day='2018-01-01,2018-01-02,2018-01-03')");<br>
 *	client.closeConnection();<br>
 * 
 * @author hzchenlei1
 *
 * 2017-8-14
 */
public class ImpalaClient {
	
	private static final Logger LOG = Logger.getLogger(ImpalaClient.class);
	
	// jdbc connection
	private static Connection conn;
	// 最大返回给客户端的数据条数
	private static final int MAX_RETURN_RECORD_SIZE = 3000;
	
	public ImpalaClient(String hiveServerUrl){
		// 建立连接
		if(null!=conn){
			return;
		}
		createConnection(hiveServerUrl);
	}
	
	private synchronized void createConnection(String url){
		if(null!=conn){
			return;
		}
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Class not found: org.apache.hive.jdbc.HiveDriver", e);
		}
		try {
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			throw new RuntimeException("Create connection failed", e);
		}
		LOG.info("Create connection to: " + url);
	}
	
	public void closeConnection(){
		if(null!=conn){
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.error("Close connection failed", e);
			}
		}
	}
	
	/**
	 * 执行查询sql
	 * @param sql
	 * @return 
	 * @throws SQLException
	 */
	public List<HashMap<String, Object>> executeDML(final String DMLSql) throws SQLException{
		if(null==DMLSql || "".equals(DMLSql)){
			LOG.warn("sql could not be null");
			return null;
		}
		if(null==conn){
			throw new RuntimeException("Connection could not be null");
		}
		
		List<HashMap<String, Object>> resultList = new ArrayList<HashMap<String, Object>>();
		
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			LOG.info("execute sql: "+DMLSql);
			stmt.executeQuery(DMLSql);
			rs = stmt.getResultSet();
			
			if(rs.getRow() > 0){
				ResultSetMetaData metaData = rs.getMetaData();
				int columCount = metaData.getColumnCount(); // 列数
				int returnSize = 0; // 返回记录条数
				while(rs.next()){
					returnSize ++;
					HashMap<String, Object> rowMap = new HashMap<String, Object>();
					for(int i=1; i<=columCount; i++){
						String key = metaData.getColumnName(i);
						String value = rs.getString(i);
						rowMap.put(key, value);
					}
					resultList.add(rowMap);
					
					// 限制返回数据集大小
					if(returnSize >= MAX_RETURN_RECORD_SIZE){
						LOG.warn("The query result size reached the max value: "+ MAX_RETURN_RECORD_SIZE + ", and discard the rest of result data.");
						break;
					}
				}
			}
			return resultList;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if(null!=rs){
					rs.close();
				}
				if(null!=stmt){
					stmt.close();
				}
			} catch (Exception e2) {
				LOG.warn(e2.getMessage());
			}
		}
	}
	
	/**
	 * 执行ddl sql语句
	 * @return
	 */
	public int executeDDL(String DDLSql){
		if(null==DDLSql || "".equals(DDLSql)){
			LOG.warn("sql could not be null");
			return -1;
		}
		if(null==conn){
			throw new RuntimeException("Connection could not be null");
		}
		
		Statement stmt = null;
		ResultSet rs = null;
		
		long startTime = System.currentTimeMillis();
		long endTime = 0l;
		
		try {
			stmt = conn.createStatement();
			LOG.info("execute sql: "+DDLSql);
			return stmt.executeUpdate(DDLSql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if(null!=rs){
					rs.close();
				}
				if(null!=stmt){
					stmt.close();
				}
			} catch (Exception e2) {
				LOG.warn(e2.getMessage());
			}
			endTime = System.currentTimeMillis();
			LOG.info("cost time: " + (endTime - startTime) + " ms");
		}
	}
	
	
	/**
	 * 针对impala table和partition进行refresh<br>
	 * <br>
	 * demo:<br>
	 *    refresh hubble.uda_events;<br>
	 *    refresh hubble.uda_users;<br>
	 *    refresh uda.uda_events Partition(day='2018-01-01,2018-01-02,2018-01-03')<br>
	 *    
	 * @param refreshSql
	 * @return
	 */
	public boolean refresh(String refreshSql){
		int result = executeDDL(refreshSql);
		if(result>=0){
			return true;
		}else{
			return false;
		}
	}
	
}




