package com.rogerguo.test;

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

    public static void verify() {
        List<String> resultList = new ArrayList<>();

        File file = new File("input.log");
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
                RangeQueryCommand command = new RangeQueryCommand(200, 500, 100, 200, 1556454237256L, 1556454237559L);
                if (command.isContainThisPoint(object)) {
                    System.out.println(object.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
