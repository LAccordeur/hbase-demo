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

    private long minTimestamp = Long.MAX_VALUE;

    private long maxTimestamp = Long.MIN_VALUE;


    public ClientCache(String zookeeperUrl, int cacheSize, int serverBlockSize) {
        this.cacheMap = new HashMap<>();
        this.cacheSize = cacheSize;
        this.serverBlockSize = serverBlockSize;
        this.server = new HBaseUtils(zookeeperUrl);

        this.index = new Index(zookeeperUrl);
    }

    public ClientCache(String zookeeperUrl, int cacheSize, int serverBlockSize, long timePeriod, boolean isStreamData) {
        this.cacheMap = new HashMap<>();
        this.cacheSize = cacheSize;
        this.serverBlockSize = serverBlockSize;
        this.server = new HBaseUtils(zookeeperUrl);

        this.index = new Index(zookeeperUrl, timePeriod, isStreamData);
    }

    public void append(KeyValuePair data) {
        SpatialTemporalRecord record = (SpatialTemporalRecord) data.getValue();
        long itemTimestamp = record.getTimestamp();
        if (itemTimestamp > maxTimestamp) {
            this.maxTimestamp = itemTimestamp;
        }

        if (itemTimestamp < minTimestamp) {
            this.minTimestamp = itemTimestamp;
        }

        cacheMap.put(data.getKey(), data.getValue());

    }

    public boolean isFlush() {
        if (cacheMap.size() >= cacheSize) {
            return true;
        }

        return false;
    }

    /**
     * flush时需要做的两件事，对数据按照区域进行分组，更新索引表
     *
     */
    public void flush() {
        //1. 进行数据分组：每个子区域用子区域内所有点的最相似前缀来标识
        // TODO 由于现阶段采用的客户端缓存模拟，服务端会再进行一次排序，而后续实现时需要考虑到Set的无序性
        long startTime = System.currentTimeMillis();
        Map<String, Object> resultData = DataUtil.groupSpatialData(cacheMap, this.serverBlockSize);
        long stopTime = System.currentTimeMillis();
        System.out.println("Group data consumes " + (stopTime - startTime) / 1000.0 + " s");

        //2. 更新index
        System.out.println("Time Min: " + DataUtil.printTimestamp(minTimestamp) + " -- " + minTimestamp);
        System.out.println("Time Max: " + DataUtil.printTimestamp(maxTimestamp) + " -- " + maxTimestamp);
        Long[] currentTimeIndex = index.update(resultData, minTimestamp, maxTimestamp);
        System.out.println();
        startTime = System.currentTimeMillis();
        sendToServer(resultData, currentTimeIndex);
        stopTime = System.currentTimeMillis();
        System.out.println("Hbase put consumes " + (stopTime - startTime) / 1000.0 + " s");

        //3. 清空缓存并校正时间记录
        cacheMap.clear();
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
    }


    private void sendToServer(Map<String, Object> dataMap, Long[] currentTimeIndex) {
        //key -> 子区域id     value -> 该子区域内的数据点list
        //将数据解析成HBase Put对象，并调用HBase API写入HBase
        Set<String> keySet = dataMap.keySet();
        List<Put> putList = new ArrayList<>();
        for (String key : keySet) {
            String rowkey = generateKey(key, currentTimeIndex);
            Put put = new Put(Bytes.toBytes(rowkey));

            List valueList = (List) dataMap.get(key);
            for (Object record : valueList) {
                SpatialTemporalRecord spatialTemporalRecord = (SpatialTemporalRecord) record;
                put.addColumn(Bytes.toBytes(Client.DATA_FAMILY), Bytes.toBytes(spatialTemporalRecord.getId()), Bytes.toBytes(spatialTemporalRecord.toString()));
            }
            putList.add(put);
        }
        server.putBatch(Client.DATA_TABLE, putList);

    }

    public static String generateKey(String spatialKey, Long[] currentTimeIndex) {

        long lastIndexKey = currentTimeIndex[0];
        long lastColumnKey = currentTimeIndex[1];

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(lastIndexKey);
        stringBuilder.append(lastColumnKey);
        stringBuilder.append(spatialKey);

        return stringBuilder.toString();
    }



    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
}
