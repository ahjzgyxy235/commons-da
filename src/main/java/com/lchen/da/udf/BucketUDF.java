package com.lchen.da.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.mortbay.log.Log;


/** 
 * 分桶UDF函数<p>
 *
 * 注意：这个分桶函数逻辑每次修改后需要同时联系以下两个模块进行同步更新
 *
 * 1> 实时数据处理写event表模块，更新新版本commons-da包重新上线运行
 * 2> 联系impala集群负责人重建udf函数, 具体步骤如下
 *    2.1> 上传commons-da包（自行操作）
 * 				hdfs dfs -put commons-da-0.13-SNAPSHOT.jar hdfs://hz-impala1/user/da/hubble/udf/
 *    2.2> 重建udf函数（由impala集群管理员操作）
 *    			create function bucketby location 'hdfs://hz-impala1/user/da/hubble/udf/commons-da-0.13-SNAPSHOT.jar'
 *    			symbol='com.netease.da.udf.BucketUDF';
 *
 * @author hzchenlei1
 *
 * 2017-4-13
 */
public class BucketUDF extends UDF {
	
	/**
	 * 根据输入参数计算当前数据所属bucket
	 * @param productId
	 * @param dataType
	 * @param eventId
	 * @return
	 */
	public String evaluate(String productId, String dataType, String eventId){
		return BucketPartition.getBucketPartitionValue(productId, dataType, eventId);
	}
	
	public static void main(String[] args) {
		BucketUDF udf = new BucketUDF();
		System.out.println(udf.evaluate("1053", "e", "buttonclick"));
	}
}

class BucketPartition {
	private static final String SESSION_BUCKETID    	   = "session"; //session事件分桶编号
	private static final String PAGE_VIEW_BUCKETID         = "pv";      //pageview事件分桶编号   
	private static final String AUTO_TRACK_BUCKETID        = "at";      //自动跟踪事件分桶编号
	private static final String AD_CLICK_BUCKETID          = "ad";      //广告点击事件分桶编号
	private static final String ABTEST_BUCKETID            = "abtest";  //abtest命中实验事件分桶编号
	private static final String CODE_TRACK_BUCKETID_PREFIX = "ct_";     //代码埋点事件分桶编号前缀
	
	/**
	 * 根据输入参数计算事件分桶层分区值(分桶编号bucketID)
	 * @param productId 必填参数 (必须为数字)
	 * @param dataType 必填参数
	 * @param eventId 必填参数
	 * @return 分区值，比如 session，pv，at, ct_0 等等
	 */
	public static String getBucketPartitionValue(String productId, String dataType, String eventId){
		if(StringUtils.isBlank(productId) || StringUtils.isBlank(dataType) || StringUtils.isBlank(eventId)){
			throw new IllegalArgumentException("Invalid input argument: productId="+productId+", dataType="+dataType+", evenId="+eventId);
		}

		//pageview事件
		if("pv".equals(dataType) && "da_screen".equals(eventId)){
			return PAGE_VIEW_BUCKETID;
		}
		//自动跟踪事件
		if("auto".equals(dataType)){
			return AUTO_TRACK_BUCKETID;
		}
		//session事件
		if("da_session_start".equals(eventId) || "da_session_close".equals(eventId)){
			return SESSION_BUCKETID;
		}
		//ad_click事件
		if("ie".equals(dataType) && "da_ad_click".equals(eventId)){
			return AD_CLICK_BUCKETID;
		}
		//da_abtest事件
		if("ie".equals(dataType) && "da_abtest".equals(eventId)){
			return ABTEST_BUCKETID;
		}
		// 代码埋点事件按照eventId hash绝对值取模作为分桶索引后缀
		int productNum = -1;
		try {
			productNum = Integer.parseInt(productId);
		} catch (Exception e) {
			Log.warn("Invalid arguments: productId "+productId);
			throw new IllegalArgumentException(e);
		}
		
		int bucketNumber = getBucketNumber(productNum);
		int buckentIndex = Math.abs(eventId.hashCode()) % bucketNumber;
		return CODE_TRACK_BUCKETID_PREFIX.concat(String.valueOf(buckentIndex));
	}
	
	/**
	 * 根据产品ID获取对应的分桶数量
	 * @param productId
	 * @return
	 */
	private static int getBucketNumber(int productId){
		int bucketNumber = 1; // 默认产品对应的分桶数量1
		
		switch (productId) {
			case 1000: bucketNumber = 10;  break; // LOFTER
			case 1001: bucketNumber = 1;   break; // UAPP
			case 1002: bucketNumber = 10;  break; // 漫画
			case 1003: bucketNumber = 1;   break; // testProduct
			case 1004: bucketNumber = 1;   break; // 网易美学
			case 1005: bucketNumber = 10;  break; // 云阅读
			case 1006: bucketNumber = 1;   break; // 云课堂
			case 1007: bucketNumber = 1;   break; // 网易蜗牛读书
			case 1009: bucketNumber = 1;   break; // 中国大学MOOC
			case 1010: bucketNumber = 1;   break; // 蜗牛读书
			case 1011: bucketNumber = 1;   break; // 网易洞见
			case 1012: bucketNumber = 10;  break; // 云音乐
			case 1014: bucketNumber = 1;   break; // 易信
			case 1018: bucketNumber = 1;   break; // 小团圆
			case 1019: bucketNumber = 1;   break; // stone
			case 1020: bucketNumber = 1;   break; // HubbleData
			case 1021: bucketNumber = 1;   break; // 网易考拉海购微商城
			case 1022: bucketNumber = 20;  break; // 网易考拉海购
			case 1023: bucketNumber = 1;   break; // BDesk
			case 1024: bucketNumber = 1;   break; // BDesk-测试
			case 1025: bucketNumber = 1;   break; // 猛犸
			case 1026: bucketNumber = 1;   break; // 网易农业
			case 1027: bucketNumber = 1;   break; // 天谕
			case 1028: bucketNumber = 1;   break; // bimkk
			case 1029: bucketNumber = 1;   break; // 旧猛犸
			case 1034: bucketNumber = 10;  break; // 梦幻藏宝阁
			case 1053: bucketNumber = 20;  break; // 网易考拉 - 直接导数据产品
			case 1079: bucketNumber = 10;  break; // 云音乐游戏中心
			default: bucketNumber = 1;     break; // 其他产品
		}
		return bucketNumber;
	}
}




