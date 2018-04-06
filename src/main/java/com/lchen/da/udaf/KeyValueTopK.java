package com.lchen.da.udaf;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


/**
 * udaf参数类型校验
 * 
 *  key-value topK udaf输入参数类型分别是
 *  0 - key    - string
 *  1 - value  - number
 *  2 - K      - int
 *  3 - order  - string
 * 
 * @author hzchenlei1
 *
 */
public class KeyValueTopK extends AbstractGenericUDAFResolver{
	
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if(parameters.length!=4){
			throw new UDFArgumentException("Invalid parameters length");
		}
		if(!parameters[0].getTypeName().toUpperCase().equals("STRING")){
			throw new UDFArgumentException("Invalid parameter type at index 0 : "+parameters[0].getTypeName());
		}
		if(!parameters[1].getTypeName().toUpperCase().equals("INT") 
				&& !parameters[1].getTypeName().toUpperCase().equals("BIGINT")
				&& !parameters[1].getTypeName().toUpperCase().equals("DOUBLE") 
				&& !parameters[1].getTypeName().toUpperCase().equals("FLOAT")){
			throw new UDFArgumentException("Invalid parameter type at index 1 : "+parameters[1].getTypeName());
		}
		if(!parameters[2].getTypeName().toUpperCase().equals("INT")){
			throw new UDFArgumentException("Invalid parameter type at index 2 : "+parameters[2].getTypeName());
		}
		if(!parameters[3].getTypeName().toUpperCase().equals("STRING")){
			throw new UDFArgumentException("Invalid parameter type at index 3 : "+parameters[3].getTypeName());
		}
		return new KeyValueTopKEvaluator();
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		return this.getEvaluator(info.getParameters());
	}
	
	public static class KeyValueTopKEvaluator extends GenericUDAFEvaluator {
		
		private static final Logger LOG = LoggerFactory.getLogger(KeyValueTopKEvaluator.class);
		
		private StringObjectInspector stringInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		private LongObjectInspector longInspector = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
		private IntObjectInspector intInspector = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		
		// 最小K值
		private static final int MIN_K = 1;
		// 最大K值
		private static final int MAX_K = 3000;
		// 默认降序排列
		private String actualOrder = "DESC";
		// 默认K值
		private int actualK = 10;
		
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		}

		/**
		 * 处理udaf输入数据并进行局部计算
		 */
		@Override
		public void iterate(AggregationBuffer aggBuffer, Object[] ins)
				throws HiveException {
			
			if(null==ins || ins.length==0){
				return;
			}
			
			String inputKey = stringInspector.getPrimitiveJavaObject(ins[0]);
			long inputValue = Long.parseLong(String.valueOf(longInspector.getPrimitiveJavaObject(ins[1])));
			int k = Integer.parseInt(String.valueOf(intInspector.getPrimitiveJavaObject(ins[2])));
			String order = stringInspector.getPrimitiveJavaObject(ins[3]);
			
			if(k >= MIN_K || k <= MAX_K){
				actualK = k;
			}
			
			actualOrder = (order.toUpperCase()).equals("ASC") ? "ASC" : "DESC";
			
			KeyValueTopKBuffer topKBuffer = (KeyValueTopKBuffer)aggBuffer;
			
			// 查找并插入新元素
			LinkedList<KeyValuePair> newStateValue = findAndInsert(topKBuffer.kvList, new KeyValuePair(inputKey, inputValue), actualK, actualOrder);
			
			// 截取topK个元素
			if(newStateValue.size() > actualK){
				topKBuffer.kvList = new LinkedList<KeyValuePair>(newStateValue.subList(0, actualK));
			}
			
		}
		
		/**
		 * 局部计算输出结果(json string)
		 */
		@Override
		public Object terminatePartial(AggregationBuffer aggBuffer) throws HiveException {
			KeyValueTopKBuffer topKBuffer = (KeyValueTopKBuffer)aggBuffer;
			JSONObject json = new JSONObject();
			for (KeyValuePair pair : topKBuffer.kvList) {
				json.put(pair.getKey(), pair.getValue());
			}
			return json.toJSONString();
		}
		
		/**
		 * 对局部计算的结果进行merge
		 */
		@Override
		public void merge(AggregationBuffer aggBuffer, Object partial)
				throws HiveException {
			KeyValueTopKBuffer topKBuffer = (KeyValueTopKBuffer)aggBuffer;
			
			if(null!=partial){
				String partialBuffer = stringInspector.getPrimitiveJavaObject(partial);
				JSONObject partitalJSON = JSONObject.parseObject(partialBuffer);
				for (String jsonKey : partitalJSON.keySet()) {
					long jsonValue = partitalJSON.getLong(jsonKey);
					KeyValuePair tmpPair = new KeyValuePair(jsonKey, jsonValue);
					// 查找并插入新元素
					LinkedList<KeyValuePair> newStateValue = findAndInsert(topKBuffer.kvList, tmpPair, actualK, actualOrder);
					// 截取topK个元素
					if(newStateValue.size() > actualK){
						topKBuffer.kvList = new LinkedList<KeyValuePair>(newStateValue.subList(0, actualK));
					}
				}
			}
		}
		
		/**
		 * 定义AggregationBuffer 保存聚合计算的中间状态结果
		 */
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			KeyValueTopKBuffer buffer = new KeyValueTopKBuffer();
			reset(buffer);
			return buffer;
		}
		
		/**
		 * 任务重跑时需要清空内存中的数据
		 */
		@Override
		public void reset(AggregationBuffer aggBuffer) throws HiveException {
			KeyValueTopKBuffer buffer = (KeyValueTopKBuffer)aggBuffer;
			buffer.kvList.clear();
		}
		
		/**
		 * 输出最终结果(json string)
		 */
		@Override
		public Object terminate(AggregationBuffer aggBuffer) throws HiveException {
			return terminatePartial(aggBuffer);
		}
		
		/**
		 * 通过二分查找插入新元素
		 * 
		 * 如果发现同key的pair，则比较两者较大值的pair并取出来，参与查找插入
		 * 如果未发现同key的pair，则直接对tmpPair进行查找插入
		 * 
		 * @param stateValue
		 * @param pair
		 * @param topK
		 * @param order
		 * @return 按value排序后的top K-V
		 */
		public static LinkedList<KeyValuePair> findAndInsert(LinkedList<KeyValuePair> stateValue, 
				   KeyValuePair pair,
				   int topK, 
				   String order){
			
			long stmp = System.currentTimeMillis();
			
			// 待插入pair
			KeyValuePair tmpPair = pair;
			
			int sameKeyPairIndex = -1;
			for (int i = 0; i < stateValue.size(); i++) {
				KeyValuePair currPair = stateValue.get(i);
				if(currPair.equalKey(pair)){ // 同key pair
					sameKeyPairIndex = i;
					break;
				}
			}
			
			// 说明发现了同key的pair，取两者value较大者，然后删掉该位置的pair再重新查找插入
			if(sameKeyPairIndex >= 0){ 
				if(stateValue.get(sameKeyPairIndex).getValue() > pair.getValue()){
					tmpPair = stateValue.get(sameKeyPairIndex);
				}
				stateValue.remove(sameKeyPairIndex);
			}
			
			// 二分查找到合适位置
			int insertIndex = find(stateValue, tmpPair, order);
			// 插入新pair
			stateValue.add(insertIndex, tmpPair);
			
			return stateValue;
		}
		
		/**
		 * 查找合适位置
		 * @param list
		 * @param pair
		 * @param order
		 * @return 正确的插入位置索引
		 */
		private static int find(List<KeyValuePair> list, KeyValuePair pair, String order){
			
			int insertIndex = 0;
			
			// 处理边界情况
			if(null==list || list.size()==0){
				return 0;
			}else if(list.size()==1){
				KeyValuePair kvPair = list.get(0);
				if("DESC".equals(order)){
					if(pair.bigEqualThan(kvPair)){
						return 0;
					}else{
						return 1;
					}
				}else if("ASC".equals(order)){
					if(pair.smallEqualThan(kvPair)){
						return 0;
					}else{
						return 1;
					}
				}
			}
			
			for (int i = 0; i < list.size(); i++) {
				int j = i + 1;
				KeyValuePair pairI = list.get(i);
				KeyValuePair pairJ = list.get(j);
				
				if(pair.equalThan(pairI)){
					insertIndex = i;
					break;
				}
				
				if(pair.equalThan(pairJ)){
					insertIndex = j;
					break;
				}
				
				if("DESC".equals(order) && pair.smallThan(pairI) && pair.bigThan(pairJ)){
					insertIndex = j;
					break;
				}
				
				if("ASC".equals(order) && pair.bigThan(pairI) && pair.smallThan(pairJ)){
					insertIndex = j;
					break;
				}
				
				// 处理边界情况
				if(i==0){
					if("DESC".equals(order) && pair.bigThan(pairI)){
						insertIndex = 0;
						break;
					}
					if("ASC".equals(order) && pair.smallThan(pairI)){
						insertIndex = 0;
						break;
					}
				}
				
				// 处理边界情况
				if(j==list.size()-1){
					if("DESC".equals(order) && pair.smallThan(pairJ)){
						insertIndex = j+1;
						break;
					}
					if("ASC".equals(order) && pair.bigThan(pairJ)){
						insertIndex = j+1;
						break;
					}
				}
			}
			
			return insertIndex;
		}
	
		
		/**
		 * udaf 中间计算结果缓存
		 */
		@GenericUDAFEvaluator.AggregationType(estimable = true)
		static class KeyValueTopKBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {
			
			protected LinkedList<KeyValuePair> kvList = new LinkedList<KeyValuePair>();
			
			// topK最多支持取3000条记录，预估最大使用5M存储空间
			public int estimate() {
	            return 5 * 1024 * 1024;
	        }
		}
		
	}
	
}




