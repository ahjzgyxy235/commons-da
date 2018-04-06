package com.lchen.da.fastmap;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class FastMapUDF extends UDF {
  
	public String evaluate(String json) {
		if (json == null || json.isEmpty()) return null;
	    try {
	      JSONObject object = JSON.parseObject(json);
	      FastMapStructure struct = new FastMapStructure();
	      byte[] binary = struct.encodeToBytesArray(object);
	      return new String(binary, "UTF-8");
	    } catch (Exception e) {
	      return null;
	    }
   }
  
   public String evaluate(Map<String, Object> map) {
	    if (map == null || map.isEmpty()) return null;
	    try {
	      FastMapStructure struct = new FastMapStructure();
	      byte[] binary = struct.encodeToBytesArray(map);
	      return new String(binary, "UTF-8");
	    } catch (Exception e) {
	      return null;
	    }
   }
}
