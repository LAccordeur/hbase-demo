package com.rogerguo.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author : guoyang
 * @Description : simulate storeFile
 * @Date : Created on 2019/4/15
 */
public class Store {

    private Map<String, Object> dataStore;

    private Set<String> spatialIndex;

    private int blockSize;

    public Store() {
        this.dataStore = new HashMap<>();
        this.blockSize = 10;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public Map<String, Object> getDataStore() {
        return dataStore;
    }

    public void setDataStore(Map<String, Object> dataStore) {
        this.dataStore = dataStore;
    }

    public void updateSpatialIndex() {
        this.spatialIndex = dataStore.keySet();
    }

    public Object getValue(String key) {
        return dataStore.get(key);
    }

    public List<String> searchIndex(RangeQueryCommand command) {


        return null;
    }
}
