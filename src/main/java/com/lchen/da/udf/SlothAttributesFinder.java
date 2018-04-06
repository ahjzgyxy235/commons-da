package com.lchen.da.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.netease.sloth.udf.UDF;

/** 
 * 从attributes字符串中解析出查询key对应的值 
 * 
 * 这是sloth udf，不适用于hive，spark，impala
 * 
 * @author hzchenlei1
 *
 * 2017-11-21
 */
public class SlothAttributesFinder implements UDF {
	
	private static final Logger LOG = Logger.getLogger(SlothAttributesFinder.class);
	
	/**
	 * 这里统一返回string类型，sloth sql中自行根据实际情况进行类型转换
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
		return attributesJson.getString(jsonKey);
	}
	
}




