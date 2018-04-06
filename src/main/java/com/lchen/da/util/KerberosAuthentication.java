package com.lchen.da.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/** 
 * kerberos认证<br><br>
 * 
 * 通过keytab文件完成当前机器的kerberos认证工作，从而获得通信票据
 * 
 *  Usage:
 *  	Configuration kconf = new Configuration();
 *		kconf.setBoolean("hadoop.security.authorization", true);
 *		kconf.set("hadoop.security.authentication", "kerberos"); 
 *  	KerberosAuthentication.authenticate(new Configuration(), "principalname", "keytab file path");
 * 
 * @author hzchenlei1
 *
 * 2016年7月13日
 */
public class KerberosAuthentication {
	
	private static final Logger LOG = Logger.getLogger(KerberosAuthentication.class);
	
	/**
	 * 通过口令和keytab文件登录
	 * @param conf hadoop集群配置
	 * @param principalName
	 * @param keytabFilePath
	 */
	public static void authenticate(Configuration conf, String principalName, String keytabFilePath){
		if(conf==null){
			conf = new Configuration();
		}
		if(StringUtils.isBlank(principalName)){
			throw new RuntimeException("principal name couldn't by null or empty!");
		}
		if(StringUtils.isBlank(keytabFilePath)){
			throw new RuntimeException("keytab file path couldn't by null or empty!");
		}
		LOG.info("Kerberos authenticates by principal name: "+principalName+" and keytab file: "+keytabFilePath);
		
		File keytabFile = new File(keytabFilePath);
		if(!keytabFile.exists()){
			throw new RuntimeException("keytab file path: "+keytabFilePath+" doesn't exist!");
		}
		// 通过keytab文件进行用户无密码登录
		try {
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(principalName, keytabFilePath);
		} catch (IOException e) {
			throw new RuntimeException("Authenticate failed!", e);
		}
		LOG.info("Kerberos authentication successed!");
	}
	
}




