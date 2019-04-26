package com.rogerguo.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/25
 */
public class HBaseAPITest {

    public static Configuration conf;

    public static Connection conn;

    public static String ZOOKEEPER_URL = "localhost";

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_URL);

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            createTable("test_api", "info");
            String[] columns = {"name1", "age1"};
            String[] values = {"rogerguo1", "181"};
            insertData("test_api", "guoyang", "info", columns, values);
            scanRow("test_api", "guoyang");
        } catch (IOException e) {
            e.printStackTrace();
        }
     }

    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        Admin admin = conn.getAdmin();

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
        admin.close();
    }

    public static void insertData(String tableName, String rowkey, String columnFamily, String[] columns, String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));

        }

        table.put(put);
        System.out.println("data inserted");
        table.close();
    }

    public static void scanRow(String tableName, String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println(rowkey +
                    " column=" + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier()) +
                    ", timestamp=" + kv.getTimestamp() + ", value=" + Bytes.toString(kv.getValue()));
        }
    }
}
