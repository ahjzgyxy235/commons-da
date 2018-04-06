package com.lchen.da.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * 序列化反序列化工具
 * @author hzchenlei1
 *
 * 2017-1-5
 */
public class SerializeUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(SerializeUtil.class);
	
	public static byte[] serialize(Object object) {
		ObjectOutputStream oos = null;
	    ByteArrayOutputStream baos = null;
	    try {
	        // 序列化
	        baos = new ByteArrayOutputStream();
	        oos = new ObjectOutputStream(baos);
	        oos.writeObject(object);
	        byte[] bytes = baos.toByteArray();
	        return bytes;
	    } catch (Exception e) {
	    	LOG.error("serialize exception", e);
	    	throw new RuntimeException(e);
	    }
   }

   public static Object unserialize( byte[] bytes) {
       ByteArrayInputStream bais = null;
       try {
           // 反序列化
           bais = new ByteArrayInputStream(bytes);
           ObjectInputStream ois = new ObjectInputStream(bais);
           return ois.readObject();
       } catch (Exception e) {
    	   LOG.error("unserialize exception", e);
    	   throw new RuntimeException(e);
       }
   }
   
   public static void main(String[] args) {
	   byte[] bytes = "陈".getBytes();
	   System.out.println("字节数组长度: "+bytes.length);
	   for (byte b : bytes) {
		   System.out.println(b);
	   }
   }
}




