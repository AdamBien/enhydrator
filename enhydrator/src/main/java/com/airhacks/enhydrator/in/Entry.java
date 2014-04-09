package com.airhacks.enhydrator.in;

/**
 *
 * @author airhacks.com
 */
public class Entry {

    private String name;
    //from java.sql.Type
    private int sqlType;
    private Object value;

    public Entry(String name, int sqlType, Object value) {
        this.name = name;
        this.sqlType = sqlType;
        this.value = value;
    }

    @Override
    public String toString() {
        return "{" + "name:" + name + ", sqlType:" + sqlType + ", value:" + value + '}';
    }

}
