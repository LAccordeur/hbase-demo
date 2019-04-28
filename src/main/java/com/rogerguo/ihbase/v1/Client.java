package com.rogerguo.ihbase.v1;

import com.rogerguo.demo.KeyValuePair;
import com.rogerguo.demo.RangeQueryCommand;
import com.rogerguo.demo.SpatialRange;
import com.rogerguo.demo.SpatialTemporalRecord;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * @Author : guoyang
 * @Description : 由于方案中的设想是在flush的过程中进行地理数据的分组聚合，而这个过程从目前找到的信息
 * 来看需要修改HBase的源码（HBase 协处理器可行性 preFlush是否能做的？），暂时的测试实现方案是先将数据缓存在客户端，并在客户端完成数据的分组聚合再提交
 * 到服务端中并同时更新索引表；这就意味着数据查询会存在一段真空期，同时在HBase的MemStore中的数据也是经过了分组组合的
 *
 * 假设：
 * 1.达到一次memory flush的时间小于时域索引中的固定时间间隔
 * 2.flush时在创建可变时间偏移量时利用到的时间精度可以保证两次连续的flush的时间戳是不同的
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

    public static String DATA_FAMILY = "info";

    private HBaseUtils server;

    public Client(String zookeeperUrl) {
        //进行初始化工作，包括建立客户端缓存、生成数据表和索引表
        this.clientCache = new ClientCache(zookeeperUrl, 200, 20);
        this.server = new HBaseUtils(zookeeperUrl);
        server.createTable(Client.DATA_TABLE, Client.DATA_FAMILY);
    }




    public static void main(String[] args) {


        RangeQueryCommand command = new RangeQueryCommand(455305418, 500000000, 205950878, 1828127042, 1556418630832L, 1556418631614L);
        new Client("127.0.0.1").batchPut();

    }

    public void batchPut() {

        try {
            //暂时以模拟数据进行替代测试，测试完成后利用nyc-taxi进行测试
            File file = new File("input.log");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter writer = new FileWriter(file, true);
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i < 1000; i++) {
                int id = i;
                int longitude = Math.abs(random.nextInt(1000));
                int latitude = Math.abs(random.nextInt(1000));
                long timestamp = System.currentTimeMillis();
                String data = String.valueOf(random.nextLong());
                SpatialTemporalRecord record = new SpatialTemporalRecord(id, latitude, longitude, timestamp, data);
                //put(record);
                writer.write(record.toString());
                writer.write("\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void put(SpatialTemporalRecord record) {
        //1. 接收数据，放入客户端缓存
        //将时空对象转换为key value，缓存中的key只含空间信息（如ZOrdering编码），value目前为直接将对象的属性转为字符串
        KeyValuePair keyValuePair = DataUtil.transferToKeyValuePair(record);
        clientCache.append(keyValuePair);

        //2. 判断客户端缓存是否满了
        if (clientCache.isFlush()) {
            clientCache.flush();
        }

    }

    public void scan(RangeQueryCommand command) {
        //由于是在客户端进行的模拟缓存，所以这个版本里不考虑内存数据中的额外对待
        //1. 根据时间范围扫描索引表
        List<Result> resultList = server.scan(Index.INDEX_TABLE, command.getTimeMin() - Index.TIME_PERIOD, command.getTimeMax());

        //2. 解析索引表记录进一步缩小范围
        Set<String> resultKeySet = parseIndexScanResult(resultList, command);

        //3. 根据读取到的聚合key读取记录，并在内存中进行解析
        for (String key : resultKeySet) {
            Result result = server.get(Client.DATA_TABLE, Bytes.toBytes(key));
            for (Cell cell : result.listCells()) {
                int id = Bytes.toInt(cell.getQualifier());
                String value = Bytes.toString(cell.getValueArray());
                ObjectMapper objectMapper = new ObjectMapper();
                SpatialTemporalRecord resultRecord = null;
                try {
                    resultRecord = objectMapper.readValue(value, SpatialTemporalRecord.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (command.isContainThisPoint(resultRecord)) {
                    System.out.println("id = " + Bytes.toInt(cell.getQualifier()) +
                            "; value = " + Bytes.toString(cell.getValue()));
                }
            }
        }
    }

    private Set<String> parseIndexScanResult(List<Result> resultList, RangeQueryCommand command) {
        Set<String> resultKeySet = new LinkedHashSet<>();

        SpatialRange rangeQuerySpatialRange = new SpatialRange(command.getLongitudeMin(), command.getLongitudeMax(), command.getLatitudeMin(), command.getLatitudeMax());
        for (Result result : resultList) {

            List<Cell> cellList = result.listCells();
            long oldTemporalColumnKey = 0;
            for (int i = 0; i < cellList.size(); i++) {
                Cell cell = cellList.get(i);
                long temporalIndexKey = Bytes.toLong(cell.getRow());
                long temporalColumnKey = Bytes.toLong(cell.getQualifier());
                String spatialIndexString = Bytes.toString(cell.getValue());

                Map<String, SpatialRange> spatialIndexMap = Index.parseSpatialIndexString(spatialIndexString);
                Set<String> spatialKeySet = spatialIndexMap.keySet();
                if (i == 0) {
                    boolean isOverlop = (command.getTimeMax() > temporalIndexKey) && (command.getTimeMin() < temporalIndexKey + temporalColumnKey);
                    if (isOverlop) {
                        // 说明时域上有交集
                        for (String spatialKey : spatialKeySet) {
                            SpatialRange spatialRange = spatialIndexMap.get(spatialKey);
                            if (spatialRange.isOverlap(rangeQuerySpatialRange)) {
                                //空间域上有交集
                                Long[] temporalIndex = {temporalIndexKey, temporalColumnKey};
                                String queryKey = ClientCache.generateKey(spatialKey, temporalIndex);
                                resultKeySet.add(queryKey);
                            }
                        }
                    }
                } else {
                    if ((command.getTimeMax() > temporalIndexKey + oldTemporalColumnKey) && (command.getTimeMin() < temporalIndexKey + temporalColumnKey)) {
                        // 说明时域上有交集
                        for (String spatialKey : spatialKeySet) {
                            SpatialRange spatialRange = spatialIndexMap.get(spatialKey);
                            if (spatialRange.isOverlap(rangeQuerySpatialRange)) {
                                Long[] temporalIndex = {temporalIndexKey, temporalColumnKey};
                                String queryKey = ClientCache.generateKey(spatialKey, temporalIndex);
                                resultKeySet.add(queryKey);
                            }
                        }
                    }
                }
                oldTemporalColumnKey = temporalColumnKey;


            }
        }

        return resultKeySet;
    }



}
