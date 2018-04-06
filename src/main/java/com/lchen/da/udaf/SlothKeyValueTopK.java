package com.lchen.da.udaf;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.netease.sloth.udaf.multiple.MultipleUDAF;

/** 
 * Key-Value键值对型数据取TopK
 * 这里仅仅排序是仅仅针对Value进行排序，排序后的K个Value连同对应的Key一起返回
 * 
 * 输入：
 * KeyValueTopK UDAF仅仅支持4个输入参数，类型分别是：
 * 		Key     不参与排序的Key, 只支持字符型
 *      Value   参与排序的Value, 只支持数值型
 *      K       计算topK的K大小, 大于1的整数型，否则取默认Top1
 *      Order   指定降序(DESC)还是升序(ASC)，字符型，默认降序排列
 * 
 * 输出:
 * json结果demo
 * {
 * 	 "K1":V1,
 * 	 "K2":V2,
 * 	 "K3":V3,
 * 	   ...
 * 	 "Kk":Vk 
 * }
 * 
 * 注：这个UDAF的存在目的仅仅是为了实现 sql中 group by + order by + limit K 语法对应的语义
 * 
 * @author hzchenlei1
 *
 */
@SuppressWarnings("serial")
public class SlothKeyValueTopK implements MultipleUDAF<Object[], LinkedList<KeyValuePair>, String>{
	
	// 最小K值
	private static final int MIN_K = 1;
	// 默认K值
	private int actualK = 10;
	// 最大K值
	private static final int MAX_K = 3000;
	// 默认降序排列
	private static final String DEFAULT_ORDER = "DESC";
	
	/**
	 * 中间计算
	 */
	@Override
	public LinkedList<KeyValuePair> accumulate(LinkedList<KeyValuePair> stateValue, Object[] ins) {
		if(null==ins || ins.length==0){
			return stateValue;
		}
		
		String inputKey = (String) ins[0];
		Long inputValue = (Long) ins[1];
		
		if((Integer)ins[2] >= MIN_K || (Integer)ins[2] <= MAX_K){
			actualK = (Integer)ins[2];
		}
		
		String order = (ins[3].toString().toUpperCase()).equals("ASC") ? "ASC" : DEFAULT_ORDER;
		
		// 查找并插入新元素
		LinkedList<KeyValuePair> newStateValue = findAndInsert(stateValue, new KeyValuePair(inputKey, inputValue), actualK, order);
		
		// 截取topK个元素
		if(newStateValue.size() > actualK){
			return new LinkedList<KeyValuePair>(newStateValue.subList(0, actualK));
		}else{
			return newStateValue;
		}
		
	}
	
	/**
	 * 初始化ACC，定义中间状态存储结构
	 */
	@Override
	public LinkedList<KeyValuePair> createAccumulator() {
		return new LinkedList<KeyValuePair>();
	}
	
	/**
	 * 返回结果
	 * 取出所有元素并转为json字符串输出
	 */
	@Override
	public String getResult(LinkedList<KeyValuePair> stateValue) {
		JSONObject json = new JSONObject();
		for (KeyValuePair pair : stateValue) {
			json.put(pair.getKey(), pair.getValue());
		}
		return json.toJSONString();
	}
	
	/**
	 * 这里没实现撤销动作
	 */
	@Override
	public LinkedList<KeyValuePair> retract(LinkedList<KeyValuePair> stateValue, Object[] ins) {
		// do nothing
		return stateValue;
	}
	
	/**
	 * 查找并插入新元素
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
	private LinkedList<KeyValuePair> findAndInsert(LinkedList<KeyValuePair> stateValue, 
			   KeyValuePair pair,
			   int topK, 
			   String order){
		
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
	
	@SuppressWarnings("rawtypes")
	@Override
	public List<Class> getParametersTypeInfo() {
		 Class[] resultType = new Class[]{String.class, Long.class, Integer.class, String.class};
	     return Arrays.asList(resultType);
	}
	
}

class KeyValuePair {  
	
	private String key;  
	private long value;  
	
	public KeyValuePair(String key, long value) {  
		this.key = key;  
		this.value = value;  
	}  
	
	/** 
	* @return the key 
	*/  
	public String getKey() {  
		return key;  
	}  
	
	/** 
	* @return the value 
	*/  
	public long getValue() {  
		return value;  
	}
	
	/**
	 * set value
	*/  
	public void setValue(long value) {  
		this.value = value;
	}
	
	/**
	 * 按照value进行比较
	 * 大于p2的value则返回true，否则false
	 * @param p2
	 * @return
	 */
	public boolean bigThan(KeyValuePair p2){
		return (value-p2.value>0) ? true : false;
	}
	
	/**
	 * 按照value进行比较
	 * 大于等于p2的value则返回true，否则false
	 * @param p2
	 * @return
	 */
	public boolean bigEqualThan(KeyValuePair p2){
		return (value-p2.value>=0) ? true : false;
	}
	
	/**
	 * 按照value进行比较
	 * 小于p2的value则返回true，否则false
	 * @param p2
	 * @return
	 */
	public boolean smallThan(KeyValuePair p2){
		return (value-p2.value<0) ? true : false;
	}
	
	/**
	 * 按照value进行比较
	 * 小于等于p2的value则返回true，否则false
	 * @param p2
	 * @return
	 */
	public boolean smallEqualThan(KeyValuePair p2){
		return (value-p2.value<=0) ? true : false;
	}
	
	/**
	 * 按照value进行比较
	 * 等于p2的value则返回true，否则false
	 * @param p2
	 * @return
	 */
	public boolean equalThan(KeyValuePair p2){
		return value==p2.value;
	}
	
	/**
	 * 按照key进行比较
	 * 两者的key相等则返回true，否则false
	 * @param p2
	 * @return
	 */
	public boolean equalKey(KeyValuePair p2){
		return key.equals(p2.key);
	}

	@Override
	public String toString() {
		return "[" + key + ","+ value + "]";
	}
	
} 
