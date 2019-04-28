package com.rogerguo.ihbase.v1;


import com.rogerguo.demo.SpatialRange;
import com.rogerguo.demo.SpatialTemporalRecord;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @Author : guoyang
 * @Description : 索引有三部分组成：第一部分为定长的时间，第二部分为非定长的时间，第三部分为空间
 * @Date : Created on 2019/4/25
 */
public class Index {

    private HBaseUtils server;

    public static String INDEX_TABLE = "taxi_data_index";

    public static String INDEX_FAMILY = "info";

    public static long TIME_PERIOD = 60 * 60 * 1000; //一个小时

    public long initIndexKey;

    public long lastIndexKey;

    public long lastColumnKey;

    public Index(String zookeeperUrl) {
        this.server = new HBaseUtils(zookeeperUrl);
        // 索引表中rowkey为定长时间段，column为非定长时间，value为空间索引
        initialize();
    }

    private void initialize() {
        server.createTable(INDEX_TABLE, INDEX_FAMILY);
        this.initIndexKey = -1;
        this.lastIndexKey = -1;
        this.lastColumnKey = -1;
    }

    /**
     * 我首先需要知道当前索引表中最新的那条时间索引中的固定时间间隔的时间戳信息，拿到这个时间戳后我需要做以下判断
     * 来进一步决定我的后续操作 -- 目前的flush的数据属于哪条时域索引记录
     * 情况1：获取flush时的时间戳信息，若时间戳与时域索引表中最新的固定间隔的时间戳差值小于固定时间间隔则，
     * 该块数据属于这个时域索引记录，只需更新可变偏移量
     *
     * 情况2：若flush时的时间戳与时域索引表中最新的固定时间间隔的时间戳大于固定时间间隔且小于两倍的固定时间间隔，
     * 则在索引表中最新的固定时间间隔时间戳上加上一个固定时间间隔的值作为一个新的索引记录
     *
     * 情况3：若flush时的时间戳与时域索引表中最新的固定时间间隔的时间吹的差值大于等于两倍的固定四件间隔，
     * 则需要将前面的索引信息也进行更新
     *
     * @param dataMap
     * @return 返回最新的时域索引key
     */
    public Long[] update(Map<String, Object> dataMap) {
        long indexKey = this.lastIndexKey;
        long columnKey = this.lastColumnKey;

        //判断时域索引的时间戳与当前时间戳的差值
        long currentTimestamp = System.currentTimeMillis();
        long timeDelta = currentTimestamp - lastIndexKey;

        if (timeDelta < TIME_PERIOD) {
            //属于情况一，直接在当前索引记录中更新时间偏移量
            long newColumnKey = timeDelta;
            columnKey = newColumnKey;
        } else if (timeDelta >= TIME_PERIOD && timeDelta < 2 * TIME_PERIOD) {
            //属于情况二，需要新建索引记录
            long newIndexKey;
            long newColumnKey;
            if (lastIndexKey != -1) {
                newIndexKey = lastIndexKey + TIME_PERIOD;
                newColumnKey = currentTimestamp - newIndexKey;
            } else {
                //第一条索引记录
                newIndexKey = currentTimestamp;
                newColumnKey = currentTimestamp - currentTimestamp;
            }
            indexKey = newIndexKey;
            columnKey = newColumnKey;
        } else {
            //属于第三种情况，补齐空的索引记录
            long count = timeDelta / TIME_PERIOD;
            List<Put> putList = new ArrayList<>();
            for (int i = 1; i < count; i++) {
                long rowkey = lastIndexKey + i * TIME_PERIOD;
                long emptyColumnKey = -1L;
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(INDEX_FAMILY), Bytes.toBytes(emptyColumnKey), Bytes.toBytes(""));
                putList.add(put);
            }

            server.putBatch(INDEX_TABLE, putList);

        }

        /*if ((currentTimestamp - lastIndexKey) >= TIME_PERIOD) {
            //1.1 需要新开一个时间段
            long newIndexKey;
            long newColumnKey;
            if (lastIndexKey != -1) {
                newIndexKey = lastIndexKey + TIME_PERIOD;
                newColumnKey = currentTimestamp - newIndexKey;

            } else {
                newIndexKey = currentTimestamp;
                newColumnKey = currentTimestamp - currentTimestamp;
            }
            indexKey = newIndexKey;
            columnKey = newColumnKey;
        } else {
            // 1.2不需要新开时间段

            long newColumnKey = currentTimestamp - lastIndexKey;
            columnKey = newColumnKey;

        }*/
        //3.生成当前的索引记录
        Put put = new Put(Bytes.toBytes(indexKey));
        String spatialIndexValue = generateSpatialRange(dataMap);
        put.addColumn(Bytes.toBytes(INDEX_FAMILY), Bytes.toBytes(columnKey), Bytes.toBytes(spatialIndexValue));
        server.put(INDEX_TABLE, put);


        this.lastIndexKey = indexKey;
        this.lastColumnKey = columnKey;
        Long[] resultIndex = {indexKey, columnKey};
        return resultIndex;
    }


    private String generateSpatialRange(Map<String, Object> dataMap) {
        Set<String> keySet = dataMap.keySet();

        StringBuilder stringBuilder = new StringBuilder();
        for (String key : keySet) {
            int longitudeMin = Integer.MAX_VALUE;
            int longitudeMax = Integer.MIN_VALUE;
            int latitudeMin = Integer.MAX_VALUE;
            int latitudeMax = Integer.MIN_VALUE;
            SpatialRange range = new SpatialRange();
            List records = (List) dataMap.get(key);
            for (Object record : records) {
                SpatialTemporalRecord spatialTemporalRecord = (SpatialTemporalRecord) record;
                if (spatialTemporalRecord.getLongitude() > longitudeMax) {
                    longitudeMax = spatialTemporalRecord.getLongitude();
                }
                if (spatialTemporalRecord.getLongitude() < longitudeMin) {
                    longitudeMin = spatialTemporalRecord.getLongitude();
                }
                if (spatialTemporalRecord.getLatitude() > latitudeMax) {
                    latitudeMax = spatialTemporalRecord.getLatitude();
                }
                if (spatialTemporalRecord.getLatitude() < latitudeMin) {
                    latitudeMin = spatialTemporalRecord.getLatitude();
                }
            }
            range.setLongitudeMin(longitudeMin);
            range.setLongitudeMax(longitudeMax);
            range.setLatitudeMin(latitudeMin);
            range.setLatitudeMax(latitudeMax);

            //TODO 改善空间索引
            stringBuilder.append(generateSpatialIndexString(key, range));
            //spatialIndex.put(key, range);

        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    public static String generateSpatialIndexString(String key, SpatialRange range) {
        StringBuilder stringBuilder = new StringBuilder();
        int longitudeMin = range.getLongitudeMin();
        int longitudeMax = range.getLongitudeMax();
        int latitudeMin = range.getLatitudeMin();
        int latitudeMax = range.getLatitudeMax();
        stringBuilder.append(key).append(":").append(longitudeMin).append(",").append(longitudeMax).append(",");
        stringBuilder.append(latitudeMin).append(",").append(latitudeMax).append(";");

        return stringBuilder.toString();
    }

    public static Map<String, SpatialRange> parseSpatialIndexString(String spatialIndexString) {

        Map<String, SpatialRange> spatialIndexMap = new LinkedHashMap<>();
        String[] rangeArray = spatialIndexString.split(";");
        for (String range : rangeArray) {
            String[] items = range.split(":");
            String key = items[0];
            String[] rangeItems = items[1].split(",");
            SpatialRange spatialRange = new SpatialRange(Integer.valueOf(rangeItems[0]), Integer.valueOf(rangeItems[1]), Integer.valueOf(rangeItems[2]), Integer.valueOf(rangeItems[3]));
            spatialIndexMap.put(key, spatialRange);
        }

        return spatialIndexMap;
    }

    public Result getLastIndexInfo() {

        Result result = server.get(INDEX_TABLE, Bytes.toBytes(lastIndexKey));
        return result;
    }
}
