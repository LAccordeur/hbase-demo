package com.rogerguo.demo;

import java.util.*;

/**
 * @Author : guoyang
 * @Description : simulate memstore of hbase
 * @Date : Created on 2019/4/14
 */
public class Cache {

    private Map<String, Object> cacheMap;

    private int cacheSize;  //缓存大小

    public Cache(int cacheSize) {
        this.cacheMap = new HashMap<>();
        this.cacheSize = cacheSize;
    }

    public void append(KeyValuePair data) {
        //先将数据放入缓存当中，暂不考虑此次的data list的过大的问题
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
     * @param store
     */
    public void flush(Store store, TemporalIndex temporalIndex) {
        //1. 进行数据分组
        Map<String, Object> resultData = DataUtil.groupSpatialData(cacheMap, store.getBlockSize());
        store.setDataStore(resultData);

        //2. 更新index
        temporalIndex.update(store);
        store.updateSpatialIndex();

        //3. 清空缓存
        cacheMap.clear();
    }

    public List<SpatialTemporalRecord> scan(RangeQueryCommand command) {

        List<SpatialTemporalRecord> resultList = new ArrayList<>();
        Set<String> keySet = cacheMap.keySet();

        for (String key : keySet) {
            int[] point = DataUtil.unzordering(key);
            int longitude = point[0];
            int latitude = point[1];
            if (latitude >= command.getLatitudeMin() && latitude <= command.getLatitudeMax()
                    && longitude >= command.getLongitudeMin() && longitude >= command.getLongitudeMax()) {
                resultList.add((SpatialTemporalRecord) cacheMap.get(key));
            }

        }

        return resultList;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
}
