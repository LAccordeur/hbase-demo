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

    private Configuration conf;

    private Connection conn;



    public HBaseUtils(String zookeeperUrl) {
        try {
            this.conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zookeeperUrl);
            this.conn = ConnectionFactory.createConnection(conf);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, String... columnFamilies) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            for (String family : columnFamilies) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
                table.addFamily(hColumnDescriptor);
            }

            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("Table exists");
            } else {
                admin.createTable(table);
                System.out.println("Table created");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void putDataBatch(String tableName, List<Put> dataList) {
        try {
            HTable hTable = new HTable(conf, tableName);
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
            HTable hTable = new HTable(conf, tableName);
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putBatch(String tableName, List<Put> putList) {
        try {
            HTable hTable = new HTable(conf, tableName);
            hTable.put(putList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Result> scan(String tableName, long startKey, long stopKey) {
        List<Result> resultList = new ArrayList<>();
        try {
            HTable hTable  = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startKey));
            scan.setStopRow(Bytes.toBytes(stopKey));
            ResultScanner results = hTable.getScanner(scan);
            for (Result result : results) {
                resultList.add(result);
            }
            return resultList;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultList;
    }

    public Result[] batchGet(String tableName, List<String> rowkeyList) {
        Result[] results = null;
        HTable hTable = null;

        List<Get> getList = new ArrayList<>();
        try {
            hTable = new HTable(conf, Bytes.toBytes(tableName));
            for (String key : rowkeyList) {
                Get get = new Get(Bytes.toBytes(key));
                getList.add(get);
            }
            results = hTable.get(getList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return results;

    }


    public Result get(String tableName, byte[] rowkey) {
        HTable htable = null;
        try {
            htable = new HTable(conf, Bytes.toBytes(tableName));
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



    private void print(List<String> list) {
        for (String item : list) {
            System.out.println(item);
        }
    }

}
