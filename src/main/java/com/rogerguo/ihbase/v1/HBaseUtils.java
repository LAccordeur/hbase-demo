package com.rogerguo.ihbase.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/8
 */
public class HBaseUtils {

    private Configuration configuration;

    private Admin admin;

    public static String FAMILY = "info";

    public HBaseUtils(String zookeeperUrl) {
        try {
            this.configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", zookeeperUrl);
            Connection connection = ConnectionFactory.createConnection(configuration);
            this.admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName) {
        try {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor info = new HColumnDescriptor(FAMILY);
            hTableDescriptor.addFamily(info);
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putDataBatch(String tableName, List<Put> dataList) {
        try {
            HTable hTable = new HTable(configuration, tableName);
            int i;
            for (i = 0; i < dataList.size() / 1000000; i++) {
                hTable.put(dataList.subList(i * 1000000, (i + 1) * 1000000));
            }
            hTable.put(dataList.subList(i * 1000000, dataList.size()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void put(String tableName, Put put) {
        try {
            HTable hTable = new HTable(configuration, tableName);
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putBatch(String tableName, List<Put> putList) {
        try {
            HTable hTable = new HTable(configuration, tableName);
            hTable.put(putList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void scan(String tableName, int startRow, int stopRow) {

        ResultScanner results = null;
        HTable hTable = null;

        long firstRowArriveTime = 0;
        try {
            hTable = new HTable(configuration, tableName);
            Scan scan = new Scan();
            //System.out.println("HTable: " + System.currentTimeMillis());


            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            List<String> valueList = new ArrayList<>();
            int count = 0;

            long startTime = System.currentTimeMillis();
            results = hTable.getScanner(scan);

            Iterator<Result> iterator = results.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                if (count == 0) {
                    firstRowArriveTime = System.currentTimeMillis() - startTime;
                }
                count++;

                NavigableMap<byte[], byte[]> map = result.getFamilyMap(FAMILY.getBytes());
                for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                    String key = Bytes.toString(entry.getKey());
                    String value = Bytes.toString(entry.getValue());
                    valueList.add(value);
                }

            }
            long stopTime = System.currentTimeMillis();
            //print(valueList);
            System.out.println("This scan consumes " + (stopTime - startTime) + "ms Size: " + valueList.size());
            System.out.println("The first row of scan arrives needs " + firstRowArriveTime + "ms");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                results.close();
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    public void get(String tableName, String rowkey) {
        HTable htable = null;
        try {
            htable = new HTable(configuration, Bytes.toBytes(tableName));
            //long startTime = System.currentTimeMillis();
            Get get = new Get(Bytes.toBytes(rowkey));
            List<String> stringList = new ArrayList<>();

            long startTime = System.currentTimeMillis();
            Result result = htable.get(get);
            if (!result.isEmpty()) {
                stringList = parseGetResult(result);
            }
            long stopTime = System.currentTimeMillis();
            //print(stringList);
            System.out.println("This get consumes " + (stopTime - startTime) + "ms " + " Size: " + stringList.size());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Result get(String tableName, byte[] rowkey) {
        HTable htable = null;
        try {
            htable = new HTable(configuration, Bytes.toBytes(tableName));
            //long startTime = System.currentTimeMillis();
            Get get = new Get(rowkey);
            List<String> stringList = new ArrayList<>();


            Result result = htable.get(get);
            return result;

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private List<String> parseGetResult(Result hbaseResult) {

        List<String> resultList = new ArrayList<>();
        NavigableMap<byte[], byte[]> map = hbaseResult.getFamilyMap(FAMILY.getBytes());
        long startTime = System.currentTimeMillis();
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            String key = Bytes.toString(entry.getKey());
            String value = Bytes.toString(entry.getValue());
            String[] valueList = value.split(";");
            resultList.addAll(new ArrayList<String>(Arrays.asList(valueList)));
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("parse data consumes " + (stopTime - startTime) + "ms");
        return resultList;
    }

    private void print(List<String> list) {
        for (String item : list) {
            System.out.println(item);
        }
    }

}
