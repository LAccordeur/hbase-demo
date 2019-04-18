package com.rogerguo.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/14
 */
public class TemporalIndex {

    private Map<Long, Store> indexMap = new HashMap<>();


    public void update(Store store) {
        indexMap.put(System.currentTimeMillis(), store);
    }

    public List<Store> searchIndex(RangeQueryCommand command) {

        return null;
    }

}
