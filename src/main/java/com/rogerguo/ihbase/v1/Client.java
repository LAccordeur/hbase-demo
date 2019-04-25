package com.rogerguo.ihbase.v1;

import com.rogerguo.demo.KeyValuePair;
import com.rogerguo.demo.SpatialTemporalRecord;

/**
 * @Author : guoyang
 * @Description : 由于方案中的设想是在flush的过程中进行地理数据的分组聚合，而这个过程从目前找到的信息
 * 来看需要修改HBase的源码（HBase 协处理器可行性 preFlush是否能做的？），暂时的测试实现方案是先将数据缓存在客户端，并在客户端完成数据的分组聚合再提交
 * 到服务端中并同时更新索引表；这就意味着数据查询会存在一段真空期，同时在HBase的MemStore中的数据也是经过了分组组合的
 *
 * 存储形式：所有时间段的数据都放在一个表中
 * 时域设计：将时间信息作为rowkey的一部分，并且采用fixed time period，在当前时间段内再根据数量进行等分
 * 空间域设计：Quad tree聚合
 *
 *
 * @Date : Created on 2019/4/25
 */
public class Client {

    private ClientCache clientCache;

    public static String DATA_TABLE = "taxi_data";

    public Client(String zookeeperUrl) {
        this.clientCache = new ClientCache(zookeeperUrl, 10000, 100);

    }




    public void main(String[] args) {

    }

    public void put(SpatialTemporalRecord record) {
        //1. 接收数据，放入客户端缓存
        KeyValuePair keyValuePair = DataUtil.transferToKeyValuePair(record);
        clientCache.append(keyValuePair);

        //2. 判断客户端缓存是否满了
        if (clientCache.isFlush()) {
            clientCache.flush();
        }

    }

}
