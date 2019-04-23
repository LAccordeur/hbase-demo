package com.rogerguo.demo;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/14
 */
public class SpatialTemporalRecord {


    private int latitude;

    private int longitude;

    private long timestamp;

    private String data;

    public SpatialTemporalRecord() {
    }

    public SpatialTemporalRecord(int latitude, int longitude, long timestamp, String data) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.data = data;
    }

    @Override
    public String toString() {
        return "SpatialTemporalRecord{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                ", timestamp=" + timestamp +
                ", data='" + data + '\'' +
                '}';
    }

    public int getLatitude() {
        return latitude;
    }

    public void setLatitude(int latitude) {
        this.latitude = latitude;
    }

    public int getLongitude() {
        return longitude;
    }

    public void setLongitude(int longitude) {
        this.longitude = longitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
