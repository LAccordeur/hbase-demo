package com.rogerguo.demo;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/15
 */
public class RangeQueryCommand {

    private int longitudeMin;

    private int longitudeMax;

    private int latitudeMin;

    private int latitudeMax;

    private long timeMin;

    private long timeMax;

    public RangeQueryCommand() {
    }

    public boolean isContainThisPoint(SpatialTemporalRecord record) {
        boolean isLongitude = record.getLongitude() >= this.longitudeMin && record.getLongitude() <= this.longitudeMax;
        boolean isLatitude = record.getLatitude() >= this.latitudeMin && record.getLatitude() <= this.latitudeMax;
        boolean isTime = record.getTimestamp() >= this.timeMin && record.getTimestamp() <= this.timeMax;
        return isLatitude && isLongitude && isTime;
    }

    public RangeQueryCommand(int longitudeMin, int longitudeMax, int latitudeMin, int latitudeMax, long timeMin, long timeMax) {
        this.longitudeMin = longitudeMin;
        this.longitudeMax = longitudeMax;
        this.latitudeMin = latitudeMin;
        this.latitudeMax = latitudeMax;
        this.timeMin = timeMin;
        this.timeMax = timeMax;
    }

    public int getLongitudeMin() {
        return longitudeMin;
    }

    public void setLongitudeMin(int longitudeMin) {
        this.longitudeMin = longitudeMin;
    }

    public int getLongitudeMax() {
        return longitudeMax;
    }

    public void setLongitudeMax(int longitudeMax) {
        this.longitudeMax = longitudeMax;
    }

    public int getLatitudeMin() {
        return latitudeMin;
    }

    public void setLatitudeMin(int latitudeMin) {
        this.latitudeMin = latitudeMin;
    }

    public int getLatitudeMax() {
        return latitudeMax;
    }

    public void setLatitudeMax(int latitudeMax) {
        this.latitudeMax = latitudeMax;
    }

    public long getTimeMin() {
        return timeMin;
    }

    public void setTimeMin(long timeMin) {
        this.timeMin = timeMin;
    }

    public long getTimeMax() {
        return timeMax;
    }

    public void setTimeMax(long timeMax) {
        this.timeMax = timeMax;
    }
}
