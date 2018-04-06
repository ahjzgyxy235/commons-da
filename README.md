## da组公共代码库

### 说明  

该库主要用于组内同学的代码复用共享，欢迎大家一起来push代码并维护这个公共库~    另外，commit代码的时候请不要忘了顺手补充一些test case和开发文档。  
  
赠人玫瑰 手留余香！    

### release note  

#### commons-da-0.11 --- 2017-11-20
1. 增加sloth相关UDF、UDAF模板
2. 增加web来源站点一二级分类UDF函数
3. 增加impala client代码，支持refresh table 等DDL操作

#### commons-da-0.4 --- 2017-04-17
1. Hubble cache模块代码迁入commons-da  

```
哈勃cache API使用demo

目前只支持基于NCR的缓存操作，具体步骤如下：  

1. 工程pom中添加依赖（见页低，公司maven私服）

2. 调用API demo

* 构造连接参数
// demo
Properties ncrProps = new Properties();
ncrProps.setProperty("ncr_online_host", "127.0.0.1");
ncrProps.setProperty("ncr_online_port", "8888");
ncrProps.setProperty("ncr_online_passwd", "123456");

* 插入缓存
// 创建 HBCache对象，此时会以synchronized的方式初始化连接池
HBCache cache = new NCRCache(ncrProps);
// 构建全局唯一的 cacheID - key，同时需要指定querySource
String cacheID = cache.createCacheID("key", QuerySource.segmentation);
// 构建本次缓存的对象 cacheResult - value
CacheResult data = new CacheResult(cacheID, "cacheValue");
// 设置查询相关参数，这里主要设置了query sql中的最大范围时间边界
data.getQueryParams().setProperty(QueryOptions.QUERY_TIMERANAGE_START_DAY, "2017-01-12");
data.getQueryParams().setProperty(QueryOptions.QUERY_TIMERANAGE_END_DAY, "2017-01-15");
// 插入缓存
cache.insert(cacheID, data);

* 更新缓存
// 创建 HBCache对象
HBCache cache = new NCRCache(ncrProps);
// 构建全局唯一的 cacheID - key，同时需要指定querySource
String cacheID = cache.createCacheID("key", QuerySource.segmentation);
// 构建本次缓存的对象 cacheResult - value
CacheResult data = new CacheResult(cacheID, "cacheValue");
// 设置查询相关参数，这里主要设置了query sql中的最大范围时间边界
data.getQueryParams().setProperty(QueryOptions.QUERY_TIMERANAGE_START_DAY, "2017-01-12");
data.getQueryParams().setProperty(QueryOptions.QUERY_TIMERANAGE_END_DAY, "2017-01-15");
// 更新缓存
cache.update(cacheID, data);

* 查询缓存
// 创建 HBCache对象
HBCache cache = new NCRCache(ncrProps);
// 构建全局唯一的 cacheID - key，同时需要指定querySource
String cacheID = cache.createCacheID("key", QuerySource.segmentation);
// 查询
CacheResult resut = cache.query(cacheID);
String cacheValue = result.getCacheValue();
long cacheTime = result.getCacheTime();
int ttlTime = result.getTtlTime();

```

#### commons-da-0.3 --- 2017-04-13
1. 增加哈勃数据存储分桶策略函数以及相应的UDF函数  

```
分桶UDF函数：bucketby  根据事件数据获取对于存储分桶编号

select bucketby("productId", "dataType", "eventId"); // 标准参数格式

使用demo  

select bucketby("1000", "pv", "da_screen");          // page view事件
select bucketby("1000", "auto", "event123");         // auto track事件
select bucketby("1000", "ie", "da_session_start");   // session事件
select bucketby("1000", "ie", "da_session_close");   // session事件
select bucketby("1000", "e", "event321");            // code track事件
select bucketby("1012", "e", "event789");            // code track事件

```

#### commons-da-0.1 --- 2016-10-12
1. 增加HBaseUtil工具类，支持全局共享连接，方便获取HBase表连接对象
2. 增加RowKeyPolicy，内置了多种hbase常用rowkey设计规则，支持业务方自定义组装rowkey
3. 增加KerberosAuthentication工具类，支持代码层完成kerberos认证动作


### git  
https://g.hz.netease.com/dap/commons-da.git  
ssh://git@g.hz.netease.com:22222/dap/commons-da.git
  
### maven  

```
<dependency>  
    <groupId>com.netease.da</groupId> 
    <artifactId>commons-da</artifactId> 
    <version>${lastest_version}</version> 
</dependency>
```
 




