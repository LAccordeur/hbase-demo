package com.rogerguo.demo;

import org.junit.Test;

import javax.xml.crypto.Data;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DataUtilTest {
    @Test
    public void unzordering() throws Exception {

        DataUtil.unzordering("0000000000000000000000000000000000000000000000000000000000000110");

    }

    @Test
    public void groupSpatialData() throws Exception {

        Map<String, Object> map = new HashMap<>();
        map.put("0000110110", "2323");
        map.put("0000000010", "4324");
        map.put("0100000101", "433413");
        map.put("1111111111", "342");
        map.put("0000000110", "4324");
        map.put("0000000011", "4324");
        map.put("0000000000", "4324");

        Map<String, Object> result = DataUtil.groupSpatialData(map, 10);

        System.out.println(result.keySet());
    }

    @Test
    public void transferToKeyValuePair() throws Exception {

        SpatialTemporalRecord record = new SpatialTemporalRecord();
        record.setLatitude(1);
        record.setLongitude(2);
        KeyValuePair pair = DataUtil.transferToKeyValuePair(record);

        System.out.println(pair);
    }

}