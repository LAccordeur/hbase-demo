package com.rogerguo.demo;

import java.util.List;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/14
 */
public class Client {

    private Server server = new Server(100);

    public void put(SpatialTemporalRecord record) {
        //1.接收数据将数据放入缓存当中
        server.sendData(record);

        //2.判断缓存中的数据量是否达到阈值，若达到阈值则对缓存中的数据按照区域进行划分

        //3.对每个划分的区域提取rowkey，构造value，写入磁盘，同时更新时序索引表
    }
}
