package com.rogerguo.demo;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/15
 */
public class Server {

    private Cache cache;

    private List<Store> storeList;

    private TemporalIndex temporalIndex;

    public Server(int cacheSize) {
        this.cache = new Cache(cacheSize);
        this.storeList = new ArrayList<>();
        this.temporalIndex = new TemporalIndex();
    }

    public void sendData(SpatialTemporalRecord record) {
        //1.先将数据放入缓存中
        KeyValuePair keyValuePair = DataUtil.transferToKeyValuePair(record);
        cache.append(keyValuePair);

        //2.判断是否需要flush
        if (cache.isFlush()){
            Store store = new Store();
            cache.flush(store, temporalIndex);
            storeList.add(store);
        }
    }

    public void scanData(RangeQueryCommand command) {

        List<SpatialTemporalRecord> result = new ArrayList<>();

        //1. 扫描缓存中的数据
        List<SpatialTemporalRecord> cacheResult = cache.scan();


        //2. 检索时域索引表，查找哪些Store在这个时间段内
        List<Store> storeList = temporalIndex.searchIndex(command);

        //3. 检索store的空间index，查看哪些group key在地理查询范围内
        for (Store store : storeList) {
            List<String> keyList = store.searchIndex(command);
            for (String key : keyList) {
                Object value = store.getValue(key);
                DataUtil.parseGroupData(value);
            }
        }


    }

}
