package com.rogerguo.common;

import com.rogerguo.data.TaxiData;
import com.rogerguo.demo.RangeQueryCommand;
import com.rogerguo.demo.SpatialTemporalRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

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

    public static void main(String[] args) {
        int value = bitNormalizedDimension(116.31412, -180D, 180D, 31);
        System.out.println(value);
        double deValue = bitDenormalizedDimension(value, -180D, 180D, 31);
        System.out.println(deValue);
    }

    /**
     * 经度的范围是-180到180，纬度的范围是-90到90
     * @param taxiData
     * @return
     */
    public static SpatialTemporalRecord transferTaxiData2SpatialTemporalRecord(TaxiData taxiData) {
        SpatialTemporalRecord record = new SpatialTemporalRecord();

        record.setId(taxiData.getMedallion() + taxiData.getHackLicense());
        record.setLongitude(bitNormalizedDimension(taxiData.getLongitude(), -180D, 180D, 31));
        record.setLatitude(bitNormalizedDimension(taxiData.getLatitude(), -90D, 90D, 31));
        record.setTimestamp(taxiData.getDate().getTime());
        record.setData(taxiData.toString());


        return record;
    }

    public static RangeQueryCommand transfer2RangeQueryCommand(double longitudeMin, double longitudeMax, double latitudeMin, double latitudeMax, String dateMinString, String dateMaxString) {
        RangeQueryCommand command = new RangeQueryCommand();
        command.setLongitudeMin(bitNormalizedDimension(longitudeMin, -180D, 180D, 31));
        command.setLongitudeMax(bitNormalizedDimension(longitudeMax, -180D, 180D, 31));
        command.setLatitudeMin(bitNormalizedDimension(latitudeMin, -90D, 90D, 31));
        command.setLatitudeMax(bitNormalizedDimension(latitudeMax, -90D, 90D, 31));

        //DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

        //Date dateMin = Date.from(LocalDateTime.parse(dateMinString, dateFormat).toInstant(ZoneOffset.UTC));
        //Date dateMax = Date.from(LocalDateTime.parse(dateMaxString, dateFormat).toInstant(ZoneOffset.UTC));
        Date dateMin = DateUtil.parseDateString(dateMinString);
        Date dateMax = DateUtil.parseDateString(dateMaxString);

        command.setTimeMin(dateMin.getTime());
        command.setTimeMax(dateMax.getTime());

        return command;
    }

    /**
     * 参考了GeoMesa转换过程
     * @param value
     * @param min
     * @param max
     * @param precision 精度范围为[1, 31]
     * @return
     */
    public static int bitNormalizedDimension(Double value, Double min, Double max, int precision) {
        Integer result = null;

        long bins = 1L << precision;
        double normalizer = bins / (max - min);

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
