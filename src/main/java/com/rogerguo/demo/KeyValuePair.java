package com.rogerguo.demo;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/4/15
 */
public class KeyValuePair {

    private String key;

    private Object value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KeyValuePair{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
