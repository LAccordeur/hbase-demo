package com.rogerguo.demo;

import java.util.List;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/14
 */
public class Client {

    private Server server = new Server(4);

    public static void main(String[] args) {
        new Client().test();
    }

    public void test() {
        SpatialTemporalRecord record = new SpatialTemporalRecord(1,1, System.currentTimeMillis(), "test1");
        put(record);
        SpatialTemporalRecord record1 = new SpatialTemporalRecord(2, 3, System.currentTimeMillis(), "tes2");
        put(record1);
        SpatialTemporalRecord record2 = new SpatialTemporalRecord(3, 3, System.currentTimeMillis(), "tes2");
        put(record2);
        SpatialTemporalRecord record3 = new SpatialTemporalRecord(33,54, System.currentTimeMillis(), "haer");
        put(record3);
        SpatialTemporalRecord record4 = new SpatialTemporalRecord(342,232, System.currentTimeMillis(), "ddaf");
        put(record4);
        SpatialTemporalRecord record5 = new SpatialTemporalRecord(221, 332, System.currentTimeMillis(), "342");
        put(record5);

        RangeQueryCommand command = new RangeQueryCommand(2, 100, 2, 100, 0, System.currentTimeMillis());
        List result = scan(command);
        display(result);
    }


    public void put(SpatialTemporalRecord record) {
        server.sendData(record);
    }

    public List<SpatialTemporalRecord> scan(RangeQueryCommand command) {
        List<SpatialTemporalRecord> result = server.scanData(command);
        return result;
    }

    public void display(List<SpatialTemporalRecord> recordList) {
        for (SpatialTemporalRecord record : recordList) {
            System.out.println(record);
        }
    }
}
