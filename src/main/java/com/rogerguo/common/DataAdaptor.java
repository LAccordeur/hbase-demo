package com.rogerguo.common;

import com.rogerguo.data.TaxiData;
import com.rogerguo.demo.SpatialTemporalRecord;

/**
 * description:
 * 数据集中的空间信息均有经纬度表示，其类型为小数
 * 而时间信息为日期类型
 * 该类的目的是对数据集中的类型进行转换 -- 将小数归一化到整数用于空间填充曲线编码，将时间信息编码为java的时间戳
 *
 * author:rogerguo
 * data:2019/04/28
 */
public class DataAdaptor {

    public static SpatialTemporalRecord transfer2SpatialTemporalRecord(TaxiData taxiData) {
        SpatialTemporalRecord record = new SpatialTemporalRecord();


        return null;
    }

    public static int bitNormalizedDimension(Double value, Double min, Double max, int precision) {
        Integer result = null;

        long bins = 1L << precision;
        double normalizer = bins / (max - min);
        double denormalizer = (max - min) / bins;

        int maxIndex = (int) bins - 1;

        if (value >= max) {
            result = maxIndex;
        } else {
            result = (int) Math.floor((value - min) * normalizer);
        }
        return result;

    }

    public static double bitDenormalizedDimension(int value, Double min, Double max, int precision) {
        Double result = null;

        long bins = 1L << precision;
        double normalizer = bins / (max - min);
        double denormalizer = (max - min) / bins;

        int maxIndex = (int) bins - 1;

        if (value >= maxIndex) {
            result = min + (maxIndex + 0.5d) * denormalizer;
        } else {
            result = min + (value + 0.5d) * denormalizer;
        }

        return result;
    }

}
