package com.rogerguo.test;

import com.rogerguo.common.DataAdaptor;
import com.rogerguo.data.TaxiData;
import com.rogerguo.demo.RangeQueryCommand;
import com.rogerguo.demo.SpatialTemporalRecord;
import com.rogerguo.ihbase.v1.Client;
import com.rogerguo.ihbase.v1.DataUtil;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VerifyUtil {

    public static void main(String[] args) {
        verifyRealData();
    }

    public static void verifyRealData() {

        int cacheSize = 10000;
        int serverBlockSize = 500;
        int timePeriod = 3 * 60 * 60 * 1000;
        boolean isStreamData = false;
        Client client = new Client("127.0.0.1", cacheSize, serverBlockSize, timePeriod, isStreamData);
        RangeQueryCommand readCommand = DataAdaptor.transfer2RangeQueryCommand(-74.003043,-73.985492,40.730136,40.732052, "2010-01-02 15:00:00", "2010-01-02 16:00:00");

        List<SpatialTemporalRecord> hbaseList = client.scan(readCommand);
        //client.get(readCommand, "20100088082010019682");
        Set<String> hbaseIdSet = new HashSet<>();
        for (SpatialTemporalRecord record : hbaseList) {
            hbaseIdSet.add(record.getId());
        }
        System.out.println("HBase set size:" + hbaseIdSet.size());
        System.out.println("---------------------------------");


        TaxiData taxiData = new TaxiData();
        List<SpatialTemporalRecord> verifyList = new ArrayList<>();
        Set<String> verifySet = new HashSet<>();
        List<TaxiData> taxiDataList = taxiData.parseData("dataset/nyc_taxi_data_1_pickup_part_aa");
        for (TaxiData taxiDataObject : taxiDataList) {
            SpatialTemporalRecord record = DataAdaptor.transferTaxiData2SpatialTemporalRecord(taxiDataObject);
            //System.out.println(record.toString());
            if (readCommand.isContainThisPoint(record)) {
                String id = record.getId();
                verifySet.add(id);
                System.out.println(DataUtil.printTimestamp(record.getTimestamp()) + ": " + record.toString());
                verifyList.add(record);
            }

        }

        System.out.println("Verify set size: " + verifySet.size());

        for (String item : hbaseIdSet) {
            if (verifySet.contains(item)) {
                verifySet.remove(item);
            }
        }

        System.out.println(hbaseList.size());
        System.out.println(verifyList.size());
        System.out.println(verifySet);
    }

    public static void verify() {
        List<String> resultList = new ArrayList<>();

        RangeQueryCommand command = new RangeQueryCommand(10, 900, 10, 900, 1556793035958L, 1556793036058L);

        int cacheSize = 40;
        int serverBlockSize = 10;
        int timePeriod = 400;
        boolean isStreamData = false;
        Client client = new Client("127.0.0.1", cacheSize, serverBlockSize, timePeriod, isStreamData);
        List<SpatialTemporalRecord> hbaseRecordList = client.scan(command);
        Set<String> hbaseIdSet = new HashSet<>();
        for (SpatialTemporalRecord record : hbaseRecordList) {
            hbaseIdSet.add(record.getId());
        }

        File file = new File("input_4.log");
        try {
            String encoding = "UTF8";
            if (file.isFile() && file.exists()) {
                InputStreamReader reader = new InputStreamReader(new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String record = null;

                while ((record = bufferedReader.readLine()) != null) {
                    resultList.add(record);
                }
                bufferedReader.close();
                reader.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("--------------------");

        for (String item : resultList) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                SpatialTemporalRecord object = objectMapper.readValue(item, SpatialTemporalRecord.class);
                //RangeQueryCommand command = new RangeQueryCommand(200, 500, 100, 200, 1556774146683L, 1556774147075L);
                if (command.isContainThisPoint(object)) {
                    System.out.println(object.toString());
                    if (hbaseIdSet.contains(object.getId())) {
                        hbaseIdSet.remove(object.getId());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println(hbaseIdSet + " size :" + hbaseIdSet.size());

    }

}
