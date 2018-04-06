package com.lchen.da.udf;

import com.lchen.da.util.impala.BytesUtil;
import com.lchen.da.util.impala.FastMapStructure;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;

/**
 * 基于二分查找的FastMap快速检索工具
 *
 * Created by hzchenlei1.
 */
public class FastMapSeek extends UDF {

    private static final Logger LOG = Logger.getLogger(FastMapSeek.class);
    private static final String ENCODE_CHARSET_NAME = "UTF-8";
    private FastMapStructure fms = new FastMapStructure();

    /**
     *  fast map 快速查找函数
     * @param fastMapStr
     * @param queryKey
     * @return
     */
    public String evaluate(String fastMapStr, String queryKey) throws UnsupportedEncodingException {
        if(null==fastMapStr) return null;
        if(null==queryKey) {
            throw new RuntimeException("key must be not null");
        }

        byte[] input = fastMapStr.getBytes();

        // check fast map format
        byte[] magicNumberBytes = BytesUtil.subArray(input, 0, 4);
        if(!"FCTS".equals(new String(magicNumberBytes, ENCODE_CHARSET_NAME))){
            throw new RuntimeException("Invalid magic number");
        }

        byte[] headerBytes = BytesUtil.subArray(input, 4, 6);
        int kvSize = BytesUtil.toShort(headerBytes);

        int metaStartIndex = 6;
        int metablockLength = 8;

        int low = 0;
        int high = kvSize - 1;

        String finalResult = null;

        while(low <= high){
            int mid = (low + high)/2;

            int metaStart = metaStartIndex + (mid * metablockLength);
            int metaEnd   = metaStart + metablockLength;
            byte[] metaBytes = BytesUtil.subArray(input, metaStart, metaEnd);
            int datablock_offset  = BytesUtil.toInt(BytesUtil.subArray(metaBytes, 0, 4));
            short data_kLength = BytesUtil.toShort(BytesUtil.subArray(metaBytes, 4, 6));
            short data_vLength = BytesUtil.toShort(BytesUtil.subArray(metaBytes, 6, 8));

            String midKey = new String(BytesUtil.subArray(input, datablock_offset, datablock_offset + data_kLength), ENCODE_CHARSET_NAME);

            // fast map 数据结构默认按K升序排列
            if(queryKey.compareTo(midKey) < 0){
                high = mid - 1;
            }else if(queryKey.compareTo(midKey) > 0){
                low = mid + 1;
            }else{
                finalResult = new String(BytesUtil.subArray(input, datablock_offset + data_kLength, datablock_offset + data_kLength + data_vLength), ENCODE_CHARSET_NAME);
                break;
            }
        }

        return finalResult;
    }

}
