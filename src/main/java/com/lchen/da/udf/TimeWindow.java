package com.lchen.da.udf;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.netease.sloth.udf.UDF;

/** 
 * @author hzchenlei1
 *
 * 2017-11-23
 */
public class TimeWindow implements UDF {
	
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMddHHmm");
	private static final Logger LOG = Logger.getLogger(TimeWindow.class);
	// UDF支持的有效窗口宽度值
	private static final List<Integer> VALID_WINDOW_LENGTH = Arrays.asList(1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60);
	// 从小到大的有序窗口边界
	private static final List<String> WINDOW_BOUNDARY = new ArrayList<String>();
	
	/**
	 * 将时间戳转换成特定窗口
	 * @param timestamp
	 * @param windowLength 可用的窗口长度分别为：1, 2, 3, 4, 5, 6, 10, 15, 20, 30, 60
	 * @return 所属窗口
	 */
	public String evaluate(long timestamp, int windowLength){
		if(timestamp <= 0){
			LOG.error("Invalid input timestamp: "+timestamp);
			return null;
		}
		if(!VALID_WINDOW_LENGTH.contains(windowLength)){
			throw new IllegalArgumentException("Window length must be in: "+VALID_WINDOW_LENGTH.toString());
		}
		
		String formatedTime = SDF.format(new Date(timestamp));
		
		// 如果窗口长度是1，那么直接返回时间自身
		if(windowLength==1){
			return new StringBuffer(formatedTime).append("-").append(windowLength).toString();
		}
		
		// 根据window length计算窗口边界
		if(WINDOW_BOUNDARY.size()==0){
			createBoundary(windowLength);
		}
		
		String yyyyMMddHH = formatedTime.substring(0, 10);
		String leftBoundary = findLeftBoundary(formatedTime.substring(10));
		
		return new StringBuffer(yyyyMMddHH).append(leftBoundary).append("-").append(windowLength).toString();
	}
	
	/**
	 * 生成窗口边界
	 * @param windowLength
	 */
	private void createBoundary(int windowLength){
		int initMin = 0;
		while(initMin < 60){
			WINDOW_BOUNDARY.add(formatMinute(initMin));
			initMin += windowLength;
		}
	}
	
	/**
	 * 根据指定分钟数寻找其对应的左边界
	 * @param mins
	 * @return 左边界
	 */
	private String findLeftBoundary(String mins){
		String leftBounday = null;
		// 如果时间窗口是60mins，那么只有一个左边界00，直接返回
		if(WINDOW_BOUNDARY.size()==1){
			return "00";
		}
		
		for(int i=0; i+1 < WINDOW_BOUNDARY.size(); i++){
			String left  = WINDOW_BOUNDARY.get(i);
			String right = WINDOW_BOUNDARY.get(i+1);
			if(mins.compareTo(left)>=0 && mins.compareTo(right)<0){
				leftBounday = left;
				break;
			}
			if(i+2==WINDOW_BOUNDARY.size()){
				leftBounday = right;
				break;
			}
		}
		return leftBounday;
	}
	
	/**
	 * 把int型分钟转换成字符型分钟
	 * @param minute
	 * @return 00, 01, 10, 22, 59
	 */
	private String formatMinute(int minute){
		if(minute < 10){
			return "0"+String.valueOf(minute);
		}else{
			return String.valueOf(minute);
		}
	}
	
}
