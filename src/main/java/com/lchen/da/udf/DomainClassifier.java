package com.lchen.da.udf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import jline.internal.InputStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

/** 
 * 网站访客来源分类器
 * @author hzchenlei1
 *
 * 2017-12-18
 */
public class DomainClassifier extends UDF{
	
	private static final Logger LOG = Logger.getLogger(DomainClassifier.class);
	
	private static final Properties SEARCH_DOMIANS = new Properties(); // 搜索站点域名
	private static final Properties SOCIAL_DOMIANS = new Properties(); // 社交站点域名
	private static final Properties VIDEO_DOMIANS  = new Properties(); // 视频站点域名
	private static final Properties LIVE_DOMIANS   = new Properties(); // 直播站点域名
	private static final Properties SHOP_DOMIANS   = new Properties(); // 电商站点域名
	private static final Properties NEWS_DOMIANS   = new Properties(); // 新闻站点域名
	
	public String evaluate(String referDomain){
		// 如果不存在refer domain 则认为是直接进入
		if(StringUtils.isBlank(referDomain)){
			return VistorSource.directEntry.toString();
		}
		
		if(NEWS_DOMIANS.size()==0){
			loadProperties();
		}
		
		// --------------------------------------------------------------
		//     考虑到除了搜索引擎以外，其他网站大多拥有很多不同的三级域名，所以在匹配时，
		//     搜索域名使用全域名匹配，其他网站使用二级域名做后缀匹配
		// --------------------------------------------------------------
		
		// 搜索
		if(SEARCH_DOMIANS.containsKey(referDomain)){
			return VistorSource.search.toString();
		}
		// 社交
		Iterator<Entry<Object, Object>> socialItor = SOCIAL_DOMIANS.entrySet().iterator();
		while(socialItor.hasNext()){
			String domain_suffix = (String)socialItor.next().getKey();
			if(referDomain.endsWith(domain_suffix)){
				return VistorSource.social.toString();
			}
		}
		// 社交
		Iterator<Entry<Object, Object>> videoItor = VIDEO_DOMIANS.entrySet().iterator();
		while(videoItor.hasNext()){
			String domain_suffix = (String)videoItor.next().getKey();
			if(referDomain.endsWith(domain_suffix)){
				return VistorSource.video.toString();
			}
		}
		// 直播
		Iterator<Entry<Object, Object>> liveItor = LIVE_DOMIANS.entrySet().iterator();
		while(liveItor.hasNext()){
			String domain_suffix = (String)liveItor.next().getKey();
			if(referDomain.endsWith(domain_suffix)){
				return VistorSource.live.toString();
			}
		}	
		// 电商
		Iterator<Entry<Object, Object>> shopItor = SHOP_DOMIANS.entrySet().iterator();
		while(shopItor.hasNext()){
			String domain_suffix = (String)shopItor.next().getKey();
			if(referDomain.endsWith(domain_suffix)){
				return VistorSource.shop.toString();
			}
		}
		// 新闻
		Iterator<Entry<Object, Object>> newsItor = NEWS_DOMIANS.entrySet().iterator();
		while(newsItor.hasNext()){
			String domain_suffix = (String)newsItor.next().getKey();
			if(referDomain.endsWith(domain_suffix)){
				return VistorSource.news.toString();
			}
		}
		
		// 如果都没有匹配到，那么就认为是其他链接进入
		return VistorSource.otherLink.toString();
	}
	
	private synchronized void loadProperties() {
		if(NEWS_DOMIANS.size()==0){
			LOG.info("Load property files");
			loadFile(SEARCH_DOMIANS, "domain/search.properties");
			loadFile(SOCIAL_DOMIANS, "domain/social.properties");
			loadFile(VIDEO_DOMIANS,  "domain/video.properties");
			loadFile(LIVE_DOMIANS,   "domain/live.properties");
			loadFile(SHOP_DOMIANS,   "domain/shop.properties");
			loadFile(NEWS_DOMIANS,   "domain/news.properties");
			LOG.info("Load property successfully!");
		}
	}
	
	private void loadFile(Properties domains, String filePath){
		InputStream in = null;
		try {
			in = this.getClass().getClassLoader().getResourceAsStream(filePath);
			domains.load(new InputStreamReader(in, "UTF-8"));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Property file not found: "+filePath, e);
		} catch (IOException e) {
			throw new RuntimeException("Load file execption", e);
		} finally {
			if(null!=in){
				try {
					in.close();
				} catch (IOException e) {
					throw new RuntimeException("Close file stream execption", e);
				}
			}
		}
	}
}

/**
 * 网站访客来源分类枚举
 */
enum VistorSource {
	directEntry, // 直接进入
	search,      // 搜索
	social,      // 社交
	news,        // 新闻
	video,       // 视频
	live,        // 直播
	shop,        // 电商
	otherLink    // 其他链接
}



