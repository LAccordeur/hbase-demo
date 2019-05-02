package com.rogerguo.test;

import com.rogerguo.common.DataAdaptor;
import com.rogerguo.data.TaxiData;
import com.rogerguo.demo.RangeQueryCommand;
import com.rogerguo.demo.SpatialTemporalRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class VerifyUtil {

    public static void main(String[] args) {
        verify();
    }

    public static void verifyRealData() {
        TaxiData taxiData = new TaxiData();
        List<TaxiData> taxiDataList = taxiData.parseData("dataset/nyc_taxi_data_1_pickup_part_aa");
        RangeQueryCommand command = DataAdaptor.transfer2RangeQueryCommand(-74.003143,-73.995492,40.730136,40.732052, "2010-01-02 15:00:00", "2010-01-02 15:35:00");
        for (TaxiData taxiDataObject : taxiDataList) {
            SpatialTemporalRecord record = DataAdaptor.transferTaxiData2SpatialTemporalRecord(taxiDataObject);
            //System.out.println(record.toString());
            if (command.isContainThisPoint(record)) {
                System.out.println(record.toString());
            }

        }
    }

    public static void verify() {
        List<String> resultList = new ArrayList<>();

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


        for (String item : resultList) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                SpatialTemporalRecord object = objectMapper.readValue(item, SpatialTemporalRecord.class);
                //RangeQueryCommand command = new RangeQueryCommand(200, 500, 100, 200, 1556774146683L, 1556774147075L);
                RangeQueryCommand command = new RangeQueryCommand(10, 900, 10, 900, 1556774146826L, 1556774147000L);

                if (command.isContainThisPoint(object)) {
                    System.out.println(object.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
