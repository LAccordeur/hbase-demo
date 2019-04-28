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
            scanDataTable("taxi_data");
            scanIndexTable("taxi_data_index");
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

    public static void scanIndexTable(String tableName) throws IOException {
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.listCells()) {
                long rowkey = Bytes.toLong(cell.getRow());
                long timeOffset = Bytes.toLong(cell.getQualifier());
                String spatialValue = Bytes.toString(cell.getValue());
                System.out.println("rowkey = " +  rowkey + "    timeOffset = " + timeOffset + "     spatial value = " + spatialValue);
            }
        }
    }

    public static void scanDataTable(String tableName) throws IOException {
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.listCells()) {
                String rowkey = Bytes.toString(cell.getRow());
                int id = Bytes.toInt(cell.getQualifier());
                String value = Bytes.toString(cell.getValue());
                System.out.println("rowkey = " +  rowkey + "    id = " + id + "     spatial temporal value = " + value);
            }
        }
    }
}
