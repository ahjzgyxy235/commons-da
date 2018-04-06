package com.lchen.da.sloth;

import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.netease.sloth.parser.TableAndMonitorParser;
import com.netease.sloth.types.RowRecord;

/** 
 * 事件log解析工具<br>
 * 1>主要是将kafka中json string按照sloth source表schema定义转换成row
 * 2>同时针对一行输入数据输出两行数据，冗余数据进行产品级别的指标计算
 * @author hzchenlei1
 * 2017-11-17
 */
public class EventLogParser extends TableAndMonitorParser{
	
	private static final Logger LOG = Logger.getLogger(EventLogParser.class);
	private static final String ATTRIBUTES_COLUMN_UPPER_KEY = "ATTRIBUTES";
	private static final String APPKEY_COLUMN_UPPER_KEY = "APPKEY";
	private JSONObject formatedJSON;

	@Override
	public Object getMonitorMsg() {
		return null;
	}

	@Override
	public void process(RowRecord inputRecord) throws Exception {
		if (inputRecord == null || inputRecord.getArity() == 0) {
            return;
        }
		
		String inputStr = (String) inputRecord.getField(0);
		try {
			formatedJSON = formatJsonKey(JSONObject.parseObject(inputStr));
		} catch (Exception e) {
			LOG.error("Invalid input record: "+inputStr);
		}
		
		if(null==formatedJSON){
			return;
		}
		
		int tableSize = tableFieldNames.length;
		
		/*
		 * 由于sloth不支持later view explode语法，所以在这里进行parse动作的时候，顺便做一次UDTF操作进行数据冗余扩充
		 * 具体是将所有数据冗余一倍，将appkey字段对应的值改为"ALL"，通过这种方式实现产品级别的指标计算 
		 */
		RowRecord outputRecord = new RowRecord(tableSize);
		RowRecord outputRecord2 = new RowRecord(tableSize);
        for (int i = 0; i < tableSize; i++) {
        	String tableFieldName = tableFieldNames[i];
        	/**
        	 * XXX 这里需要注意的是，由于sloth不支持嵌套数据结构，所以这里的attributes转成json string
        	 *     后续在sloth sql中如果需要使用到attributes内部的字段，可以通过自定义udf来实现
        	 */
        	if(tableFieldName.equals(ATTRIBUTES_COLUMN_UPPER_KEY)){  
        		JSONObject jsonObj = formatedJSON.getJSONObject(ATTRIBUTES_COLUMN_UPPER_KEY);
        		if(null!=jsonObj){
        			outputRecord.setField(i, jsonObj.toJSONString());
        			outputRecord2.setField(i, jsonObj.toJSONString());
        		}else{
        			outputRecord.setField(i, null);
        			outputRecord2.setField(i, null);
        		}
        	}else if(tableFieldName.equals(APPKEY_COLUMN_UPPER_KEY)){
        		outputRecord.setField(i, formatedJSON.get(tableFieldName));
        		outputRecord2.setField(i, "ALL");
        	}else if(tableFieldName.equals("USERID")){
        		outputRecord.setField(i, formatedJSON.get(tableFieldName).toString());
        		outputRecord2.setField(i, formatedJSON.get(tableFieldName).toString());
        	}else{
        		outputRecord.setField(i, formatedJSON.get(tableFieldName));
        		outputRecord2.setField(i, formatedJSON.get(tableFieldName));
        	}
        }
        out.collect(outputRecord);
        out.collect(outputRecord2);
	}
	
	public static void main(String[] args) {
		Integer i = 100;
		System.out.println(String.valueOf(i));
	}
	
	/**
	 * 格式化json key<br>
	 * 由于sloth传过来的schema字段都是大写的，所以parse的时候需要将json key全转成大写，保证字段映射正确
	 * @param originJson
	 * @return 大写key的json
	 */
	private JSONObject formatJsonKey(JSONObject originJson){
		JSONObject newJson = new JSONObject();
		Set<String> keySet = originJson.keySet();
		Iterator<String> itor = keySet.iterator();
		while (itor.hasNext()) {
			String originKey = itor.next();
			newJson.put(originKey.toUpperCase(), originJson.get(originKey));
		}
		return newJson;
	}
	
}




