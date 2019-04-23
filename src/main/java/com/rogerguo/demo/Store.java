package com.rogerguo.demo;

import java.util.*;

/**
 * @Author : guoyang
 * @Description : simulate storeFile
 * @Date : Created on 2019/4/15
 */
public class Store {

    private Map<String, Object> dataStore;

    private Map<String, SpatialRange> spatialIndex;

    private int blockSize;

    public Store(int blockSize) {
        this.dataStore = new HashMap<>();
        this.spatialIndex = new HashMap<>();
        this.blockSize = blockSize;
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
        Set<String> keySet = dataStore.keySet();

        for (String key : keySet) {
            int longitudeMin = Integer.MAX_VALUE;
            int longitudeMax = Integer.MIN_VALUE;
            int latitudeMin = Integer.MAX_VALUE;
            int latitudeMax = Integer.MIN_VALUE;
            SpatialRange range = new SpatialRange();
            List records = (List) dataStore.get(key);
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

            spatialIndex.put(key, range);
        }

    }

    public Object getValue(String key) {
        return dataStore.get(key);
    }

    public List<String> searchIndex(RangeQueryCommand command) {

        List<String> result = new ArrayList<>();
        Set<String> keySet = spatialIndex.keySet();
        for (String key : keySet) {
            SpatialRange range = spatialIndex.get(key);
            if (range.isOverlap(new SpatialRange(command.getLongitudeMin(), command.getLongitudeMax(), command.getLatitudeMin(), command.getLatitudeMax()))) {
                result.add(key);
            }

        }

        return result;
    }
}
