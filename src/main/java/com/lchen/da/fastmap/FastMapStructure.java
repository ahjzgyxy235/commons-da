package com.lchen.da.fastmap;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * fast map 编解码器<br>
 * 将map转化为自定义支持快速查找的数据结构<br>
 * 
 * 数据结构:
 *    magic number  +  header  +  metablock  + datablock
 *       4bytes        2bytes     N * 8bytes    N * kv
 *  
 *  1> magic number: 固定为4个字节的FCTS （fast complex type structure）<br>
 *  2> header: 固定2个字节，表示kv的size，支持map最大kv个数：32768个<br>
 *  3> metablock: kvOffset + kLength + vLength<br>
 *  4> datablock: key + value 默认按key升序排列<br>
 *
 * fast map快速检索工具参考:  {@link FastMapSeek}
 * 
 * @author hzchenlei1
 *
 */
public class FastMapStructure {
	
	private static final Logger LOG = LoggerFactory.getLogger(FastMapStructure.class);
	
	// 全局使用UTF-8编码
	private static final String ENCODE_CHARSET_NAME = "UTF-8";
	
	// 定义4个字节的magic number
	private static final String MAGIC_NUM = "FCTS";
	private static final int MAGIC_NUM_BYTES_LENGTH = 4;
	
	// 定义2个字节长度的header
	private static final int HEADER_BYTES_LENGTH = 2;
	private short headerKVSize = 0;
	
	// 转换出的字节数组
	private int totalByteLength;
	private byte[] totalBytes;
	
	/**
	 * 编码器: 根据输入map数据，按照UTF-8编码转化成自定义字节数组
	 * 转换时，按照K进行升序排列
	 * @param map 
	 * @return byte[] 转化出来的字节数组。如果输入map为空，则返回null
	 * @throws UnsupportedEncodingException
	 */
	public byte[] encodeToBytesArray(Map<String, Object> map) throws UnsupportedEncodingException{
		if(null==map || map.size()==0){
			return null;
		}

		totalByteLength = 0;

		// map按照key进行排序，升序排列
		TreeMap<String, Object> sortedMap = sortByKey(map);
		System.out.println(sortedMap);
		headerKVSize = (short)sortedMap.size();
		
		// 计算总字节数，初始化字节数组
		totalByteLength += MAGIC_NUM_BYTES_LENGTH;
		totalByteLength += HEADER_BYTES_LENGTH;
		totalByteLength += getKVMetaBlockBytesLength();
		totalBytes = new byte[totalByteLength];

		// 填充magic number 和  header
		BytesUtil.copyBytes(totalBytes, 0, MAGIC_NUM.getBytes(ENCODE_CHARSET_NAME));
		byte[] headerBytes = BytesUtil.fromShort(headerKVSize);
		BytesUtil.copyBytes(totalBytes, MAGIC_NUM_BYTES_LENGTH, headerBytes);
		
		// 填充metablock 和 datablock
		int startMetaBlockOffset = MAGIC_NUM_BYTES_LENGTH + HEADER_BYTES_LENGTH;
		int startDataBlockOffset = MAGIC_NUM_BYTES_LENGTH + HEADER_BYTES_LENGTH + getKVMetaBlockBytesLength();
		Set<Entry<String, Object>> entrySet = sortedMap.entrySet();
		for (Entry<String, Object> entry : entrySet) {
			// 创建metablock，并填充数据
			int kLength = entry.getKey().getBytes(ENCODE_CHARSET_NAME).length;
			int vLength = String.valueOf(entry.getValue()).getBytes(ENCODE_CHARSET_NAME).length;
			
			MetaBlock metaBlock = new MetaBlock(startDataBlockOffset, (short)kLength, (short)vLength);
			byte[] metaBytes = metaBlock.toBytes();
			BytesUtil.copyBytes(totalBytes, startMetaBlockOffset, metaBytes);
			startMetaBlockOffset += metaBytes.length;
			
			// 创建对应的datablock，并填充数据
			DataBlock dataBlock = new DataBlock(entry.getKey(), entry.getValue());
			byte[] dataBytes = dataBlock.toBytes();
			BytesUtil.copyBytes(totalBytes, startDataBlockOffset, dataBytes);
			startDataBlockOffset += dataBytes.length;
		}
		return this.totalBytes;
	}
	
	/**
	 * 编码器：根据输入map数据，转成UTF8编码下的字符串
	 * @param map
	 * @return string or null
	 * @throws UnsupportedEncodingException
	 */
	public String encodeToUTF8String(Map<String, Object> map) throws UnsupportedEncodingException{
		byte[] bytes = encodeToBytesArray(map);
		if(null==bytes){
			return null;
		}
		return new String(bytes, ENCODE_CHARSET_NAME);
	}

	/*
	 * 解码器：根据输入的字节数组，解出原始map
	 *
	 * 如果属于无效fast map结构则抛出异常
	 * @param bytes
	 * @return map or null
	 * @throws UnsupportedEncodingException
	 */
	public Map<String, Object> decodeFromBytesArray(byte[] bytes) throws UnsupportedEncodingException{
		if(null==bytes) return null;

		// check fast map format
		byte[] magicNumberBytes = BytesUtil.subArray(bytes, 0, 4);
		if(!MAGIC_NUM.equals(new String(magicNumberBytes, ENCODE_CHARSET_NAME))){
			throw new RuntimeException("Invalid magic number");
		}

		byte[] headerBytes = BytesUtil.subArray(bytes, 4, 6);
		int kvSize = BytesUtil.toShort(headerBytes);

		Map<String, Object> output = new HashMap<String, Object>();

		int metaStartIndex = magicNumberBytes.length + headerBytes.length;
		int metablockLength = MetaBlock.META_BYTES_LENGTH;
		for(int i=1; i<=kvSize; i++){
			int metaStart = metaStartIndex + ((i-1) * metablockLength);
			int metaEnd   = metaStart + metablockLength;
			byte[] metaBytes = BytesUtil.subArray(bytes, metaStart, metaEnd);
			int datablock_offset  = BytesUtil.toInt(BytesUtil.subArray(metaBytes, 0, 4));
			short data_kLength = BytesUtil.toShort(BytesUtil.subArray(metaBytes, 4, 6));
			short data_vLength = BytesUtil.toShort(BytesUtil.subArray(metaBytes, 6, 8));

			String key = new String(BytesUtil.subArray(bytes, datablock_offset, datablock_offset + data_kLength));
			String value = new String(BytesUtil.subArray(bytes, datablock_offset + data_kLength, datablock_offset + data_kLength + data_vLength));

			output.put(key, value);
		}

		return output;
	}

	/*
	 * 解码器：根据输入的字符串，解出原始map
	 * @param bytes
	 * @return map or null
	 * @throws UnsupportedEncodingException
	 */
	public Map<String, Object> decodeFromString(String str) throws UnsupportedEncodingException{
		if(null==str) return null;
		return decodeFromBytesArray(str.getBytes());
	}

	/**
	 * 单个metablock固定长度
	 * 所以metablock总长度 = kv数 * 单个metablock字节长度
	 * @return
	 */
	private int getKVMetaBlockBytesLength(){
		return headerKVSize * MetaBlock.META_BYTES_LENGTH;
	}
	
	/**
	 * 利用treeMap对kv进行排序，同时计算datablock的总字节长度
	 * @param map
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	private TreeMap<String, Object> sortByKey(Map<String, Object> map) throws UnsupportedEncodingException{
		TreeMap<String, Object> sortedMap = new TreeMap<String, Object>();
		Set<Entry<String, Object>> entrySet = map.entrySet();
		for (Entry<String, Object> entry : entrySet) {
			// 计算数据块字节数长度
			int kLength = entry.getKey().getBytes(ENCODE_CHARSET_NAME).length;
			int vLength = String.valueOf(entry.getValue()).getBytes(ENCODE_CHARSET_NAME).length;
			// metablock中使用2个字节存储key和value的字节长度，所以要求key-value的字节长度不能超过限制，如果超过，则直接丢弃该key-value
			if(kLength > 32767 || vLength > 32767){
				LOG.warn("The length of key-value byte array is too large, drop this key-value pair. Key="+entry.getKey() + " Value="+entry.getValue());
				continue;
			}
			
			totalByteLength += kLength;
			totalByteLength += vLength;
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws IOException {
		FastMapStructure fms = new FastMapStructure();
		// 编码
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("k1", "v1");
		map.put("k2", "v2");
		map.put("k3", "v3");
		map.put("k4", "v4");
		map.put("k5", "v5");
		map.put("k6", "v6");
		map.put("k7", "v7");
		map.put("k8", "v8");
		map.put("k9", "v9");
		map.put("1", 1);
		map.put("true", true);
		map.put("陈磊", "哈罗");
		map.put("李四", 123);

		String fcts = fms.encodeToUTF8String(map);
		System.out.println(BytesUtil.toStringFormat(fms.totalBytes));

		// 解码
		byte[] input = new byte[]{70,67,84,83,13,0,110,0,0,0,1,0,1,0,112,0,0,0,2,0,2,0,116,0,0,0,2,0,2,0,120,0,0,0,2,0,2,0,124,0,0,0,2,0,2,0,0,1,0,0,2,0,2,0,4,1,0,0,2,0,2,0,8,1,0,0,2,0,2,0,12,1,0,0,2,0,2,0,16,1,0,0,2,0,2,0,20,1,0,0,4,0,4,0,28,1,0,0,6,0,3,0,37,1,0,0,6,0,6,0,49,49,107,49,118,49,107,50,118,50,107,51,118,51,107,52,118,52,107,53,118,53,107,54,118,54,107,55,118,55,107,56,118,56,107,57,118,57,116,114,117,101,116,114,117,101,-27,-122,-81,-27,-82,-121,49,50,51,-23,-103,-120,-25,-93,-118,-27,-109,-120,-25,-67,-105};
		System.out.println(fms.decodeFromBytesArray(input));

		// 二分查找
		FastMapSeek seek = new FastMapSeek();
		System.out.println(seek.evaluate(fcts, "k2"));
	}
	
}

/**
 * 数据块，里面记录的实际的key和value字节信息
 * 
 * @author hzchenlei1
 */
class DataBlock {
	
	private static final String ENCODE_NAME = "UTF-8";
	
	private String key;
	private Object value;
	
	public DataBlock(String key, Object value) {
		super();
		this.key = key;
		this.value = value;
	}
	
	public byte[] toBytes() throws UnsupportedEncodingException{
		byte[] kBytes = key.getBytes(ENCODE_NAME);
		byte[] vBytes = String.valueOf(value).getBytes(ENCODE_NAME);
		
		byte[] bytes = new byte[kBytes.length + vBytes.length];
		BytesUtil.copyBytes(bytes, 0, kBytes);
		BytesUtil.copyBytes(bytes, kBytes.length, vBytes);
		
		return bytes;
	}
}

/**
 * metablock包含三块
 * kvOffSet - 映射的datablock起始offset - 4个字节表示   要求整个map kv长度加起来小于 2147483647 个字节
 * kLegnth  - 映射的datablock key的长度    - 2个字节表示   要求key最大长度32767个字节
 * vLegnth  - 映射的datablock value长度   - 2个字节表示   要求key最大长度32767个字节
 * 
 * @author hzchenlei1
 */
class MetaBlock {
	public static final int META_BYTES_LENGTH = 8;
	private int kvOffSet;
	private short kLegnth;
	private short vLegnth;
	
	public MetaBlock(int kvOffSet, short kLegnth, short vLegnth){
		super();
		this.kvOffSet = kvOffSet;
		this.kLegnth  = kLegnth;
		this.vLegnth  = vLegnth;
	}
	
	/**
	 * 转换固定格式成字节数组
	 * @return
	 */
	public byte[] toBytes(){
		byte[] bytes = new byte[META_BYTES_LENGTH];
		
		byte[] offsetBytes  = BytesUtil.fromInt(kvOffSet);
		byte[] kLegnthBytes = BytesUtil.fromShort(kLegnth);
		byte[] vLegnthBytes = BytesUtil.fromShort(vLegnth);
		
		BytesUtil.copyBytes(bytes, 0, offsetBytes);
		BytesUtil.copyBytes(bytes, 4, kLegnthBytes);
		BytesUtil.copyBytes(bytes, 6, vLegnthBytes);
		
		return bytes;
	}
}




