package com.rogerguo.demo;

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
                //TODO 对于数据分布不均情况下的优化
                groupMap.put(prefix, partitionKeySet);
                return;
            }
        }

        return;
    }



    public static void parseGroupData(Object data) {

    }

    public static KeyValuePair transferToKeyValuePair(SpatialTemporalRecord record) {

        KeyValuePair result = new KeyValuePair();

        int latitude = record.getLatitude();
        int longitude = record.getLongitude();

        String key = zordering(longitude, latitude);

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

        byte[] rh = toBytes(zh);
        byte[] rl = toBytes(zl);
        System.arraycopy(rh, 0, ret, 0, 4);
        System.arraycopy(rl, 0, ret, 4, 4);
        return bytesToBit(ret);

    }


    /**
     * Convert an int value to a byte array
     * @param val value
     * @return the byte array
     */
    private static byte[] toBytes(int val) {
        byte [] b = new byte[4];
        for(int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
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
