package com.rogerguo.ihbase.v1;

import com.rogerguo.demo.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @Author : guoyang
 * @Description : 客户端缓存，主要工作为将分好组的数据提交给 HBase，并且更新索引
 * @Date : Created on 2019/4/25
 */
public class ClientCache {
    private Map<String, Object> cacheMap;

    private int cacheSize;  //缓存大小

    private int serverBlockSize;  //每个Subspace的大小

    private HBaseUtils server;

    private Index index;

    private static String FAMILY = "info";
    public ClientCache(String zookeeperUrl, int cacheSize, int serverBlockSize) {
        this.cacheMap = new HashMap<>();
        this.cacheSize = cacheSize;
        this.serverBlockSize = serverBlockSize;
        this.server = new HBaseUtils(zookeeperUrl);
    }

    public void append(KeyValuePair data) {
        cacheMap.put(data.getKey(), data.getValue());

    }

    public boolean isFlush() {
        if (cacheMap.size() > cacheSize) {
            return true;
        }

        return false;
    }

    /**
     * flush时需要做的两件事，对数据按照区域进行分组，更新索引表
     *
     */
    public void flush() {
        //1. 进行数据分组
        Map<String, Object> resultData = DataUtil.groupSpatialData(cacheMap, this.serverBlockSize);
        sendToServer(resultData);

        //2. 更新index
        index.update(resultData);

        //3. 清空缓存
        cacheMap.clear();
    }

    private void sendToServer(Map<String, Object> dataMap) {
        //key -> 子区域id     value -> 该子区域内的数据点list
        //将数据解析成HBase Put对象，并调用HBase API写入HBase
        Set<String> keySet = dataMap.keySet();
        List<Put> putList = new ArrayList<>();
        for (String key : keySet) {
            String rowkey = generateKey(key);
            Put put = new Put(Bytes.toBytes(rowkey));

            List valueList = (List) dataMap.get(key);
            for (Object record : valueList) {
                SpatialTemporalRecord spatialTemporalRecord = (SpatialTemporalRecord) record;
                put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(spatialTemporalRecord.getId()), Bytes.toBytes(spatialTemporalRecord.toString()));
            }
        }
        server.putBatch(Client.DATA_TABLE, putList);

    }

    private String generateKey(String key) {
        Result result = index.getLastIndexInfo();
        result.getFamilyMap(Bytes.toBytes(Index.INDEX_FAMILY));

        //TODO
        long lastIndexKey = 0;

        StringBuilder stringBuilder = new StringBuilder();
        if ((System.currentTimeMillis() - lastIndexKey) < Index.TIME_PERIOD) {
            stringBuilder.append(lastIndexKey);
        } else {
            stringBuilder.append(lastIndexKey + Index.TIME_PERIOD);
        }
        stringBuilder.append(System.currentTimeMillis());
        stringBuilder.append(key);


        return stringBuilder.toString();
    }



    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
}
