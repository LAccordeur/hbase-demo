package com.rogerguo.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void flush(Store store, TemporalIndex index) {
        //1. 进行数据分组
        Map<String, Object> resultData = DataUtil.groupSpatialData(cacheMap, store.getBlockSize());
        store.setDataStore(resultData);

        //2. 更新index
        index.update(store);
        store.updateSpatialIndex();
    }

    public List<SpatialTemporalRecord> scan() {
        return null;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
}
