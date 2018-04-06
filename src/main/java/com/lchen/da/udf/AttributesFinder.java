package com.lchen.da.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;

/** 
 * 从attributes字符串中解析出查询key对应的值 
 * 
 * 适用于hive，spark，impala
 * 
 * @author hzchenlei1
 *
 * 2017-11-21
 */
public class AttributesFinder extends UDF {
	
	private static final Logger LOG = Logger.getLogger(AttributesFinder.class);
	
	/**
	 * 根据jsonKey解出jsonValue
	 * @param attributesStr
	 * @param jsonKey
	 * @return
	 */
	public String evaluate(String attributesStr, String jsonKey){
		if(StringUtils.isBlank(attributesStr)){
			return null;
		}
		if(StringUtils.isBlank(jsonKey)){
			LOG.error("Empty json key!");
			return null;
		}
		JSONObject attributesJson = null;
		
		try {
			attributesJson = JSONObject.parseObject(attributesStr);
		} catch (Exception e) {
			LOG.error("Invalid attributes!");
			return null;
		}
		if(null==attributesJson){
			return null;
		}
		return attributesJson.get(jsonKey).toString();
	}
	
}




