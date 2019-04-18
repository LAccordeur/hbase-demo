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

    private int timeMin;

    private int timeMax;

    public RangeQueryCommand() {
    }

    public RangeQueryCommand(int longitudeMin, int longitudeMax, int latitudeMin, int latitudeMax, int timeMin, int timeMax) {
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

    public int getTimeMin() {
        return timeMin;
    }

    public void setTimeMin(int timeMin) {
        this.timeMin = timeMin;
    }

    public int getTimeMax() {
        return timeMax;
    }

    public void setTimeMax(int timeMax) {
        this.timeMax = timeMax;
    }
}
