package com.rogerguo.demo;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/14
 */
public class SpatialTemporalRecord {

    private String id;

    private int longitude;

    private int latitude;

    private long timestamp;

    private String data;

    //private String zorderingString;

    public SpatialTemporalRecord() {
    }


    public SpatialTemporalRecord(int latitude, int longitude, long timestamp, String data) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.data = data;
    }

    public SpatialTemporalRecord(String id, int latitude, int longitude, long timestamp, String data) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.data = data;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
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
