package com.rogerguo.demo;

import java.util.*;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/14
 */
public class TemporalIndex {

    private Map<Long, Store> indexMap = new LinkedHashMap<>();

    private long lastTimestamp;

    public void update(Store store) {
        long currentTimestamp = System.currentTimeMillis();
        indexMap.put(currentTimestamp, store);
        lastTimestamp = currentTimestamp;
    }

    public List<Store> searchIndex(RangeQueryCommand command) {
        long timeMin = command.getTimeMin();
        long timeMax = command.getTimeMax();

        List<Store> result = new ArrayList<>();
        Set<Long> keySet = indexMap.keySet();
        for (Long key : keySet) {
            if (key >= timeMin && key <= timeMax) {
                result.add(indexMap.get(key));
            }
        }

        return result;
    }

    public boolean checkCacheTemporalRange(RangeQueryCommand command) {

        long timeMax = command.getTimeMax();

        if (timeMax > lastTimestamp) {
            return true;
        }

        return false;
    }
}
