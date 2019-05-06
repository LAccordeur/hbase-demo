package com.rogerguo.ihbase.v1;

import com.rogerguo.common.DataAdaptor;
import com.rogerguo.common.DateUtil;
import com.rogerguo.data.TaxiData;
import com.rogerguo.demo.KeyValuePair;
import com.rogerguo.demo.RangeQueryCommand;
import com.rogerguo.demo.SpatialRange;
import com.rogerguo.demo.SpatialTemporalRecord;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
 * 3.假设每个time period均有数据流入（目前的实现，如果没有这个假设则查询结果可能会遗漏部分数据）
 *
 * 存储形式：所有时间段的数据都放在一个表中
 * 时域设计：将时间信息作为rowkey的一部分，并且采用fixed time period，在当前时间段内再根据数量进行等分
 * 空间域设计：Quad tree聚合
 *
 * TODO 目前实现中两种可能造成查询结果偏少的情况
 * 1. 某个time period内没有数据流入且这个time period为查询范围的最后一个time period
 * 2. 查询范围与缓存中数据有交集（由于缓存中的最后一批数据在实现时未强制flush）
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
        //需要保证cachesize的大小小于一个time period内接受的数据量
        this.clientCache = new ClientCache(zookeeperUrl, 10000, 1000, 3 * 60 * 60 * 1000, false);
        //this.clientCache = new ClientCache(zookeeperUrl, 200, 20);
        this.server = new HBaseUtils(zookeeperUrl);
        server.createTable(Client.DATA_TABLE, Client.DATA_FAMILY);
    }

    public Client(String zookeeperUrl, int cacheSize, int serverBlockSize, int timePeriod, boolean isStreamData) {
        this.clientCache = new ClientCache(zookeeperUrl, cacheSize, serverBlockSize, timePeriod, isStreamData);
        this.server = new HBaseUtils(zookeeperUrl);
        server.createTable(Client.DATA_TABLE, Client.DATA_FAMILY);
    }

    public Client(String zookeeperUrl, int cacheSize, int serverBlockSize, int timePeriod, boolean isStreamData, String dataTable, String indexTable) {
        Client.DATA_TABLE = dataTable;
        Index.INDEX_TABLE = indexTable;
        this.clientCache = new ClientCache(zookeeperUrl, cacheSize, serverBlockSize, timePeriod, isStreamData);
        this.server = new HBaseUtils(zookeeperUrl);
        server.createTable(Client.DATA_TABLE, Client.DATA_FAMILY);
    }




    public static void main(String[] args) {



        RangeQueryCommand command = new RangeQueryCommand(200, 500, 100, 200, 1556774146683L, 1556774147075L);
        //空白期时间段
        RangeQueryCommand command1 = new RangeQueryCommand(10, 999, 10, 900, 1556774147220L, 1556774147225L);
        //包含空白期的时间段
        RangeQueryCommand command2 = new RangeQueryCommand(10, 900, 10, 900, 1556774146826L, 1556774147000L);

        RangeQueryCommand readCommand = DataAdaptor.transfer2RangeQueryCommand(-74.003043,-73.985492,40.730136,40.732052, "2010-01-02 15:00:00", "2010-01-02 16:00:00");

        int cacheSize = 10000;
        int serverBlockSize = 500;
        int timePeriod = 3 * 60 * 60 * 1000;
        //int cacheSize = 40;
        //int serverBlockSize = 10;
        //int timePeriod = 400;
        boolean isStreamData = false;
        Client client = new Client("127.0.0.1", cacheSize, serverBlockSize, timePeriod, isStreamData);
        client.batchPutTaxiData();

        //client.batchPutFromLog();
        //client.scan(command1);
        //client.batchPutTaxiData();
        /*long startTime = System.currentTimeMillis();
        client.scan(DataAdaptor.transfer2RangeQueryCommand(-74.003143,-73.995492,40.730136,40.732052, "2010-01-02 15:00:00", "2010-01-02 15:35:00"));
        long endTime = System.currentTimeMillis();
        System.out.println("Scan consumes " + (endTime - startTime) / 1000.0 + " s" );*/
    }

    public void batchPutTaxiData() {
        TaxiData taxiData = new TaxiData();
        long startTime = System.currentTimeMillis();
        List<TaxiData> taxiDataList = taxiData.parseData("dataset/nyc_taxi_data_1_pickup_part_aa");
        long stopTime = System.currentTimeMillis();
        System.out.println("Parse Taxi data consumes " + (stopTime - startTime) / 1000.0 + " s");
        putTaxiData(taxiDataList);
    }

    private void putTaxiData(List<TaxiData> taxiDataList) {
        long startTime = System.currentTimeMillis();
        for (TaxiData taxiData : taxiDataList) {
            SpatialTemporalRecord record = DataAdaptor.transferTaxiData2SpatialTemporalRecord(taxiData);
            //System.out.println(record.toString());
            put(record);
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("Transfer and Put taxi data to hbase parts consumes " + (stopTime - startTime) / 1000 + " s");
    }

    public void batchPut() {

        try {
            //暂时以模拟数据进行替代测试，测试完成后利用nyc-taxi进行测试
            File file = new File("input_4.log");
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter writer = new FileWriter(file, true);
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i <= 2000; i++) {
                int id = i;
                int longitude = Math.abs(random.nextInt(1000));
                int latitude = Math.abs(random.nextInt(1000));
                long timestamp = System.currentTimeMillis();
                String data = String.valueOf(Math.abs(random.nextLong()));
                SpatialTemporalRecord record = new SpatialTemporalRecord(String.valueOf(id), latitude, longitude, timestamp, data);
                put(record);
                writer.write(record.toString());
                writer.write("\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void batchPutFromLog() {
        try {
            //暂时以模拟数据进行替代测试，测试完成后利用nyc-taxi进行测试
            File file = new File("input_4.log");
            if (!file.exists()) {
                file.createNewFile();
            }

            String encoding = "UTF8";
            if (file.isFile() && file.exists()) {
                InputStreamReader reader = new InputStreamReader(new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String record = null;

                while ((record = bufferedReader.readLine()) != null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    SpatialTemporalRecord object = objectMapper.readValue(record, SpatialTemporalRecord.class);
                    put(object);
                }
                bufferedReader.close();
                reader.close();
            }

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

    public List<SpatialTemporalRecord> scan(RangeQueryCommand command) {
        //由于是在客户端进行的模拟缓存，所以这个版本里不考虑内存数据中的额外对待
        //1. 根据时间范围扫描索引表(范围是放大了的)
        long startTime = System.currentTimeMillis();
        List<Result> resultList = server.scan(Index.INDEX_TABLE, command.getTimeMin() - Index.TIME_PERIOD, command.getTimeMax() + Index.TIME_PERIOD);
        long stopTime = System.currentTimeMillis();
        System.out.println("Scan index consumes " + (stopTime - startTime) / 1000.0 + " s");

        //2. 解析索引表记录进一步缩小范围
        startTime = System.currentTimeMillis();
        List<String> resultKeySet = parseIndexScanResult(resultList, command);
        stopTime = System.currentTimeMillis();
        System.out.println("Parse index result consumes " + (stopTime - startTime) / 1000.0 + " s");

        System.out.println("Get size : " + resultKeySet.size());

        //3. 根据读取到的聚合key读取记录，并在内存中进行解析
        List<SpatialTemporalRecord> recordList = new ArrayList();
        startTime = System.currentTimeMillis();
        Result[] getResultList = server.batchGet(Client.DATA_TABLE, resultKeySet);
        ObjectMapper objectMapper = new ObjectMapper();
        for (Result result : getResultList) {
            //Result result = server.get(Client.DATA_TABLE, Bytes.toBytes(key));
            for (Cell cell : result.listCells()) {
                String id = Bytes.toString(cell.getQualifier());
                String value = Bytes.toString(cell.getValue());

                SpatialTemporalRecord resultRecord = null;
                try {
                    resultRecord = objectMapper.readValue(value, SpatialTemporalRecord.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (command.isContainThisPoint(resultRecord)) {
                    recordList.add(resultRecord);
                    //System.out.println(DataUtil.printTimestamp(resultRecord.getTimestamp()) + ": id = " + id +"; value = " + DataAdaptor.transferSpatialTemporalRecord2TaxiData(resultRecord).toString());
                    //System.out.println(DataUtil.printTimestamp(resultRecord.getTimestamp()) + ": id = " + id +"; value = " + (resultRecord).toString());

                }
            }
        }
        System.out.println("total size: " + recordList.size());
        stopTime = System.currentTimeMillis();
        System.out.println("Get and Parse data result consumes " + (stopTime - startTime) / 1000.0 + " s");

        return recordList;
    }

    public List<SpatialTemporalRecord> get(RangeQueryCommand command, String recordId) {
        //由于是在客户端进行的模拟缓存，所以这个版本里不考虑内存数据中的额外对待
        //1. 根据时间范围扫描索引表(范围是放大了的)
        List<Result> resultList = server.scan(Index.INDEX_TABLE, command.getTimeMin() - Index.TIME_PERIOD, command.getTimeMax() + Index.TIME_PERIOD);

        //2. 解析索引表记录进一步缩小范围
        List<String> resultKeySet = parseIndexScanResult(resultList, command);

        //3. 根据读取到的聚合key读取记录，并在内存中进行解析
        List<SpatialTemporalRecord> recordList = new ArrayList();
        for (String key : resultKeySet) {
            Result result = server.get(Client.DATA_TABLE, Bytes.toBytes(key));
            for (Cell cell : result.listCells()) {
                String id = Bytes.toString(cell.getQualifier());
                String value = Bytes.toString(cell.getValue());
                ObjectMapper objectMapper = new ObjectMapper();
                SpatialTemporalRecord resultRecord = null;
                try {
                    resultRecord = objectMapper.readValue(value, SpatialTemporalRecord.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (id == recordId) {
                    System.out.println("*********" + resultRecord.toString());
                }
            }
        }
        System.out.println("total size: " + recordList.size());

        return recordList;
    }

    /*private Set<String> parseIndexScanResult(List<Result> resultList, RangeQueryCommand command) {
        Set<String> resultKeySet = new LinkedHashSet<>();

        SpatialRange rangeQuerySpatialRange = new SpatialRange(command.getLongitudeMin(), command.getLongitudeMax(), command.getLatitudeMin(), command.getLatitudeMax());

        for (int j = 0; j < resultList.size(); j++) {
            Result result = resultList.get(j); //Result result : resultList
            List<Cell> cellList = result.listCells();
            long oldTemporalColumnKey = 0;
            for (int i = 0; i < cellList.size(); i++) {
                Cell cell = cellList.get(i);
                long temporalIndexKey = Bytes.toLong(cell.getRow());
                long temporalColumnKey = Bytes.toLong(cell.getQualifier());
                String spatialIndexString = Bytes.toString(cell.getValue());

                if (temporalColumnKey != -1) {
                    //TODO 思考索引检索效率是否有提高空间？ 某些情况下可以不用再继续往下检索
                    Map<String, SpatialRange> spatialIndexMap = Index.parseSpatialIndexString(spatialIndexString);
                    Set<String> spatialKeySet = spatialIndexMap.keySet();
                    if (i == 0) {
                        boolean isOverlap = (command.getTimeMax() >= temporalIndexKey) && (command.getTimeMin() <= temporalIndexKey + temporalColumnKey);
                        if (isOverlap) {
                            // 说明时域上有交集
                            getResultKey(resultKeySet, rangeQuerySpatialRange, temporalIndexKey, temporalColumnKey, spatialIndexMap, spatialKeySet);
                        }
                    } else if (i == cellList.size() - 1) {

                        boolean isOverlap = (command.getTimeMax() >= temporalIndexKey + oldTemporalColumnKey) && (command.getTimeMin() <= temporalIndexKey + temporalColumnKey);
                        if (isOverlap) {
                            // 说明时域上有交集
                            getResultKey(resultKeySet, rangeQuerySpatialRange, temporalIndexKey, temporalColumnKey, spatialIndexMap, spatialKeySet);
                        } else {
                            //最后一个时间偏移量后的空白时间段的记录在下一条索引记录的第一条中
                            if (command.getTimeMax() > temporalIndexKey + temporalColumnKey) {
                                //判断是否存在下一个索引记录 TODO ##BUG##潜在点 下个时间段没有数据时的情况
                                if (j + 1 < resultList.size()) {
                                    Result nextResult = resultList.get(j + 1);
                                    Cell nextCell = nextResult.listCells().get(0);
                                    String nextSpatialIndexString = Bytes.toString(nextCell.getValue());
                                    Map<String, SpatialRange> nextSpatialIndexMap = Index.parseSpatialIndexString(nextSpatialIndexString);
                                    Set<String> nextSpatialKeySet = nextSpatialIndexMap.keySet();
                                    getResultKey(resultKeySet, rangeQuerySpatialRange, Bytes.toLong(nextCell.getRow()), Bytes.toLong(nextCell.getQualifier()), nextSpatialIndexMap, nextSpatialKeySet);
                                }
                            }
                        }
                    } else {
                        boolean isOverlap = (command.getTimeMax() >= temporalIndexKey + oldTemporalColumnKey) && (command.getTimeMin() <= temporalIndexKey + temporalColumnKey);
                        if (isOverlap) {
                            // 说明时域上有交集
                            getResultKey(resultKeySet, rangeQuerySpatialRange, temporalIndexKey, temporalColumnKey, spatialIndexMap, spatialKeySet);
                        }
                    }
                }
                oldTemporalColumnKey = temporalColumnKey;


            }
        }

        return resultKeySet;
    }*/

    public List<String> parseIndexScanResult(List<Result> resultList, RangeQueryCommand command) {
        List<String> resultKeyList = new ArrayList<>();

        SpatialRange rangeQuerySpatialRange = new SpatialRange(command.getLongitudeMin(), command.getLongitudeMax(), command.getLatitudeMin(), command.getLatitudeMax());

        for (int j = 0; j < resultList.size(); j++) {
            Result result = resultList.get(j); //Result result : resultList
            List<Cell> cellList = result.listCells();
            long oldTemporalColumnKey = 0;
            for (int i = 0; i < cellList.size(); i++) {
                Cell cell = cellList.get(i);
                long temporalIndexKey = Bytes.toLong(cell.getRow());
                long temporalColumnKey = Bytes.toLong(cell.getQualifier());
                String spatialIndexString = Bytes.toString(cell.getValue());

                if (temporalColumnKey != -1) {
                    //TODO 思考索引检索效率是否有提高空间？ 某些情况下可以不用再继续往下检索
                    Map<String, SpatialRange> spatialIndexMap = Index.parseSpatialIndexString(spatialIndexString);
                    Set<String> spatialKeySet = spatialIndexMap.keySet();
                    if (i == 0) {
                        boolean isOverlap = (command.getTimeMax() >= temporalIndexKey) && (command.getTimeMin() <= temporalIndexKey + temporalColumnKey);
                        if (isOverlap) {
                            // 说明时域上有交集
                            getResultKey(resultKeyList, rangeQuerySpatialRange, temporalIndexKey, temporalColumnKey, spatialIndexMap, spatialKeySet);
                        }
                    } else if (i == cellList.size() - 1) {

                        boolean isOverlap = (command.getTimeMax() >= temporalIndexKey + oldTemporalColumnKey) && (command.getTimeMin() <= temporalIndexKey + temporalColumnKey);
                        if (isOverlap) {
                            // 说明时域上有交集
                            getResultKey(resultKeyList, rangeQuerySpatialRange, temporalIndexKey, temporalColumnKey, spatialIndexMap, spatialKeySet);
                        } else {
                            //最后一个时间偏移量后的空白时间段的记录在下一条索引记录的第一条中
                            if (command.getTimeMax() > temporalIndexKey + temporalColumnKey) {
                                //判断是否存在下一个索引记录 TODO ##BUG##潜在点 下个时间段没有数据时的情况
                                if (j + 1 < resultList.size()) {
                                    Result nextResult = resultList.get(j + 1);
                                    Cell nextCell = nextResult.listCells().get(0);
                                    String nextSpatialIndexString = Bytes.toString(nextCell.getValue());
                                    Map<String, SpatialRange> nextSpatialIndexMap = Index.parseSpatialIndexString(nextSpatialIndexString);
                                    Set<String> nextSpatialKeySet = nextSpatialIndexMap.keySet();
                                    getResultKey(resultKeyList, rangeQuerySpatialRange, Bytes.toLong(nextCell.getRow()), Bytes.toLong(nextCell.getQualifier()), nextSpatialIndexMap, nextSpatialKeySet);
                                }
                            }
                        }
                    } else {
                        boolean isOverlap = (command.getTimeMax() >= temporalIndexKey + oldTemporalColumnKey) && (command.getTimeMin() <= temporalIndexKey + temporalColumnKey);
                        if (isOverlap) {
                            // 说明时域上有交集
                            getResultKey(resultKeyList, rangeQuerySpatialRange, temporalIndexKey, temporalColumnKey, spatialIndexMap, spatialKeySet);
                        }
                    }
                }
                oldTemporalColumnKey = temporalColumnKey;


            }
        }

        return resultKeyList;
    }

    /*private void getResultKey(Set<String> resultKeySet, SpatialRange rangeQuerySpatialRange, long temporalIndexKey, long temporalColumnKey, Map<String, SpatialRange> spatialIndexMap, Set<String> spatialKeySet) {
        for (String spatialKey : spatialKeySet) {
            SpatialRange spatialRange = spatialIndexMap.get(spatialKey);
            if (spatialRange.isOverlap(rangeQuerySpatialRange)) {
                //空间域上有交集
                Long[] temporalIndex = {temporalIndexKey, temporalColumnKey};
                String queryKey = ClientCache.generateKey(spatialKey, temporalIndex);
                resultKeySet.add(queryKey);
            }
        }
    }*/

    private void getResultKey(List<String> resultKeyList, SpatialRange rangeQuerySpatialRange, long temporalIndexKey, long temporalColumnKey, Map<String, SpatialRange> spatialIndexMap, Set<String> spatialKeySet) {
        for (String spatialKey : spatialKeySet) {
            SpatialRange spatialRange = spatialIndexMap.get(spatialKey);
            if (spatialRange.isOverlap(rangeQuerySpatialRange)) {
                //空间域上有交集
                Long[] temporalIndex = {temporalIndexKey, temporalColumnKey};
                String queryKey = ClientCache.generateKey(spatialKey, temporalIndex);
                resultKeyList.add(queryKey);
            }
        }
    }

}
