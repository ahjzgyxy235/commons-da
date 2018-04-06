package com.lchen.da.fastmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/** 
 * 字节数组操作工具类
 * 
 * @author hzchenlei1
 *
 * 2018-1-30
 */
public class BytesUtil {
	
	/**
	 * fromBytes完全拷贝到toBytes中
	 * @param toBytes
	 * @param startIndex toBytes中的起始插入索引
	 * @param fromBytes
	 */
	public static void copyBytes(byte[] toBytes, int startIndex, byte[] fromBytes){
		
		if(toBytes.length < fromBytes.length){
			throw new RuntimeException("copy failed");
		}
		
		if(startIndex<0 || (startIndex + fromBytes.length>toBytes.length)){
			throw new RuntimeException("copy failed, invalid start index");
		}
		
		System.arraycopy(fromBytes, 0, toBytes, startIndex, fromBytes.length);
	}
	
	/**
	 * 从原数组中按照range截取生成新数组
	 * @param baseBytes
	 * @param fromIndex 包含
	 * @param toIndex  不包含
	 * @return
	 */
	public static byte[] subArray(byte[] baseBytes, int fromIndex, int toIndex){
		if(fromIndex<0 || fromIndex>baseBytes.length){
			throw new RuntimeException("Invalid from index");
		}
		if(toIndex<fromIndex || toIndex>baseBytes.length){
			throw new RuntimeException("Invalid to index");
		}
		
		byte[] results = new byte[toIndex-fromIndex];
		
		System.arraycopy(baseBytes, fromIndex, results, 0, results.length);
		
		return results;
	}

	public static String toStringFormat(byte[] bytes) {
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		for(int i=0; i<bytes.length; i++){
			if(i==bytes.length-1){
				sb.append(bytes[i]);
			}else{
				sb.append(bytes[i]).append(",");
			}
		}
		sb.append("}");
		return sb.toString();
	}
	
	/***
	  * 压缩GZip
	  * 
	  * @param data
	  * @return
	  */
	 public static byte[] gZip(byte[] data) {
	  byte[] b = null;
	  try {
	   ByteArrayOutputStream bos = new ByteArrayOutputStream();
	   GZIPOutputStream gzip = new GZIPOutputStream(bos);
	   gzip.write(data);
	   gzip.finish();
	   gzip.close();
	   b = bos.toByteArray();
	   bos.close();
	  } catch (Exception ex) {
	   ex.printStackTrace();
	  }
	  return b;
	 }

	/***
	  * 解压GZip
	  * 
	  * @param data
	  * @return
	  */
	 public static byte[] unGZip(byte[] data) {
	  byte[] b = null;
	  try {
	   ByteArrayInputStream bis = new ByteArrayInputStream(data);
	   GZIPInputStream gzip = new GZIPInputStream(bis);
	   byte[] buf = new byte[1024];
	   int num = -1;
	   ByteArrayOutputStream baos = new ByteArrayOutputStream();
	   while ((num = gzip.read(buf, 0, buf.length)) != -1) {
	    baos.write(buf, 0, num);
	   }
	   b = baos.toByteArray();
	   baos.flush();
	   baos.close();
	   gzip.close();
	   bis.close();
	  } catch (Exception ex) {
	   ex.printStackTrace();
	  }
	  return b;
	 }
	 
	 
	 /**
	  * 把字节数组转换成16进制字符串
	  * @param bArray
	  * @return
	  */
	 public static String toHexString(byte[] bArray) {
	  StringBuffer sb = new StringBuffer(bArray.length);
	  String sTemp;
	  for (int i = 0; i < bArray.length; i++) {
	   sTemp = Integer.toHexString(0xFF & bArray[i]);
	   if (sTemp.length() < 2)
	    sb.append(0);
	   sb.append(sTemp.toUpperCase());
	  }
	  return sb.toString();
	 }
	 
	
	 public static byte[] fromShort(short num) {
		  byte[] bytes = new byte[2];
		  for (int i = 0; i < bytes.length; i++) {
		    bytes[i] = (byte) (num & 0x7F) ;
		    num >>>= 7;
		  }
		  return bytes;
	 }

	 public static byte[] fromInt(int num) {
		  byte[] bytes = new byte[4];
		  for (int i = 0; i < bytes.length; i++) {
		    bytes[i] = (byte) (num & 0x7F);
		    num >>>= 7;
		  }
		  return bytes;
	 }

	public static short toShort(byte[] bytes) {
	  if (bytes == null) return 0;
	  short num = 0;
	  for (int i = 0; i < bytes.length && i < 2; i++) {
	    num += (bytes[i] << (7 * i));
	  }
	  return num;
	}

	public static int toInt(byte[] bytes) {
	  if (bytes == null) return 0;
	  int num = 0;
	  for (int i = 0; i < bytes.length && i < 4; i++) {
	    num += (bytes[i] << (7 * i));
	  }
	  return num;
	}
}




