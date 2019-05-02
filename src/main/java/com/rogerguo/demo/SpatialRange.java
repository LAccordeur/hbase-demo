package com.rogerguo.demo;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/19
 */
public class SpatialRange {

    private int longitudeMin;

    private int longitudeMax;

    private int latitudeMin;

    private int latitudeMax;

    public boolean isOverlap(SpatialRange anotherRange) {
        boolean isLongitudeOverlap = longitudeMax >= anotherRange.longitudeMin && longitudeMin <= anotherRange.longitudeMax;
        boolean isLatitudeOverlap = latitudeMax >= anotherRange.latitudeMin && latitudeMin <= anotherRange.latitudeMax;
        if (isLongitudeOverlap && isLatitudeOverlap) {
            return true;
        }
        return false;
    }

    public SpatialRange() {
    }

    public SpatialRange(int longitudeMin, int longitudeMax, int latitudeMin, int latitudeMax) {
        this.longitudeMin = longitudeMin;
        this.longitudeMax = longitudeMax;
        this.latitudeMin = latitudeMin;
        this.latitudeMax = latitudeMax;
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
}
