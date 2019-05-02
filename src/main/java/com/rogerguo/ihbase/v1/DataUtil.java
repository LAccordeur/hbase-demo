package com.rogerguo.ihbase.v1;

import com.rogerguo.demo.KeyValuePair;
import com.rogerguo.demo.RangeQueryCommand;
import com.rogerguo.demo.SpatialTemporalRecord;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/15
 */
public class DataUtil {

    /**
     * 根据quad tree来分组数据
     * @param recordList
     * @return
     */
    public static Map<String, Object> groupSpatialData(Map<String, Object> recordList, int blockSize) {
        Map<String, Object> result = new HashMap<>();

        Set<String> keySet = recordList.keySet();
        Map<String, Set<String>> groupMap = new HashMap<>();
        partition(keySet, "00", blockSize, groupMap);
        partition(keySet, "01", blockSize, groupMap);
        partition(keySet, "10", blockSize, groupMap);
        partition(keySet, "11", blockSize, groupMap);

        Set<String> prefixKeys = groupMap.keySet();
        for (String key : prefixKeys) {
            Set<String> resultKeys = groupMap.get(key);
            List aggregationValue = new ArrayList();  //Aggregation Value目前通过List来聚合
            for (String resultKey : resultKeys) {
                aggregationValue.add(recordList.get(resultKey));
            }
            result.put(key, aggregationValue);

        }

        return result;
    }

    private static void partition(Set<String> keySet, String prefix, int blockSize, Map<String, Set<String>> groupMap) {

        if (prefix.length() > 64) {
            return;
        }

        int count = 0;
        Set<String> partitionKeySet = new HashSet<>();
        for (String key : keySet) {
            if (key.startsWith(prefix)) {
                partitionKeySet.add(key);
                count++;
            }
        }

        if (count > blockSize) {
            partition(partitionKeySet, prefix + "00", blockSize, groupMap);
            partition(partitionKeySet, prefix + "01", blockSize, groupMap);
            partition(partitionKeySet, prefix + "10", blockSize, groupMap);
            partition(partitionKeySet, prefix + "11", blockSize, groupMap);
        } else {
            if (count == 0) {
                return;
            } else {
                //TODO 对于数据分布不均情况下的优化 -- 提高分辨率
                groupMap.put(prefix, partitionKeySet);
                return;
            }
        }

        return;
    }

    public static String printTimestamp(long timestamp) {

        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        return (sdf.format(date));
    }



    public static void parseGroupData(Object data, RangeQueryCommand command, List result) {
        List dataList = (List) data;
        for (Object item : dataList) {
            SpatialTemporalRecord record = (SpatialTemporalRecord) item;
            int longitude = record.getLongitude();
            int latitude = record.getLatitude();

            boolean isInLongitude = longitude >= command.getLongitudeMin() && longitude <= command.getLongitudeMax();
            boolean isInLatitude = latitude >= command.getLatitudeMin() && latitude <= command.getLongitudeMax();
            if (isInLongitude && isInLatitude) {
                result.add(record);
            }
        }

    }

    public static KeyValuePair transferToKeyValuePair(SpatialTemporalRecord record) {

        //TODO ##BUG## 潜在点 同一时间段同一个子空间内如果两个点坐标完全一样会出现覆盖的情况
        KeyValuePair result = new KeyValuePair();

        int longitude = record.getLongitude();
        int latitude = record.getLatitude();

        String key = zordering(longitude, latitude);

        record.setZorderingString(key);
        result.setKey(key);
        result.setValue(record);

        return result;
    }

    /**
     * come from md-hbase
     * @param x
     * @param y
     * @return
     */
    public static String zordering(int x, int y) {

        byte[] ret = new byte[8];
        int xh = makeGap(x);
        int xl = makeGap(x << 16);
        int yh = makeGap(y) >>> 1;
        int yl = makeGap(y << 16) >>> 1;

        int zh = xh | yh;
        int zl = xl | yl;

        byte[] rh = Bytes.toBytes(zh);
        byte[] rl = Bytes.toBytes(zl);
        System.arraycopy(rh, 0, ret, 0, 4);
        System.arraycopy(rl, 0, ret, 4, 4);
        return bytesToBit(ret);

    }


    public static int[] unzordering(String key) {
        byte[] bs = parseBinaryString(key);

        int zh = Bytes.toInt(bs, 0);
        int zl = Bytes.toInt(bs, 4);

        int xh = elimGap(zh);
        int yh = elimGap(zh << 1);
        int xl = elimGap(zl) >>> 16;
        int yl = elimGap(zl << 1) >>> 16;


        int x = xh | xl;
        int y = yh | yl;
        return new int[] { x, y };
    }


    private static final int[] MASKS = new int[] { 0xFFFF0000, 0xFF00FF00,
            0xF0F0F0F0, 0xCCCCCCCC, 0xAAAAAAAA };

    private static int makeGap(int x) {
        int x0 = x & MASKS[0];
        int x1 = (x0 | (x0 >>> 8)) & MASKS[1];
        int x2 = (x1 | (x1 >>> 4)) & MASKS[2];
        int x3 = (x2 | (x2 >>> 2)) & MASKS[3];
        int x4 = (x3 | (x3 >>> 1)) & MASKS[4];
        return x4;
    }

    private static int elimGap(int x) {
        int x0 = x & MASKS[4];
        int x1 = (x0 | (x0 << 1)) & MASKS[3];
        int x2 = (x1 | (x1 << 2)) & MASKS[2];
        int x3 = (x2 | (x2 << 4)) & MASKS[1];
        int x4 = (x3 | (x3 << 8)) & MASKS[0];
        return x4;
    }


    public static byte[] parseBinaryString(String binaryString) {
        int len = binaryString.length();

        if (len > 0 && len <=64) {
            for (int i = len; i <=64; i++) {
                binaryString = binaryString + "0";
            }
        }

        byte[] byteResult = new byte[8];

        for (int i = 0; i < 8; i++) {
            byteResult[i] = decodeBinaryString(binaryString.substring(8 * i, 8 * (i + 1)));
        }

        return byteResult;
    }

    /**
     * 将二进制字符串转为byte
     * @param byteStr
     * @return
     */
    public static byte decodeBinaryString(String byteStr) {
        int re, len;
        if (null == byteStr) {
            return 0;
        }
        len = byteStr.length();
        if (len != 4 && len != 8) {
            return 0;
        }
        if (len == 8) {// 8 bit处理
            if (byteStr.charAt(0) == '0') {// 正数
                re = Integer.parseInt(byteStr, 2);
            } else {// 负数
                re = Integer.parseInt(byteStr, 2) - 256;
            }
        } else {// 4 bit处理
            re = Integer.parseInt(byteStr, 2);
        }
        return (byte) re;
    }

    /**
     * 将byte数组转换为字符串
     * @param b
     * @return
     */
    private static String bytesToBit(byte[] b) {
        StringBuilder stringBuilder = new StringBuilder();
        for (byte item : b) {
            stringBuilder = stringBuilder.append(byteToBit(item));
        }

        return stringBuilder.toString();
    }

    /**
     * 将byte转为二进制字符串
     * @param b
     * @return
     */
    private static String byteToBit(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
    }

}
