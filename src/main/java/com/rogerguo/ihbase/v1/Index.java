package com.rogerguo.ihbase.v1;


import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

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

    public Index(String zookeeperUrl) {
        this.server = new HBaseUtils(zookeeperUrl);
    }

    public void update(Map<String, Object> dataMap) {
        //1.检测是否需要新开一个时间段, TODO 暂时假设每个小时都有数据

        if ((System.currentTimeMillis() - lastIndexKey) >= TIME_PERIOD) {

        } else {
            //2.检测在当前时间段内属于第几个子时间段

        }
        //3.生成索引记录
    }

    public Result getLastIndexInfo() {

        Result result = server.get(INDEX_TABLE, Bytes.toBytes(lastIndexKey));
        return result;
    }
}
