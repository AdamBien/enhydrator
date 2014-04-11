package com.airhacks.enhydrator.in;

/**
 *
 * @author airhacks.com
 */
public class Entry {

    private int slot;
    private String name;
    //from java.sql.Type
    private int sqlType;
    private Object value;

    public Entry(int slot, String name, int sqlType, Object value) {
        this.slot = slot;
        this.name = name.toLowerCase();
        this.sqlType = sqlType;
        this.value = value;
    }

    public int getSlot() {
        return slot;
    }

    public String getName() {
        return name;
    }

    public int getSqlType() {
        return sqlType;
    }

    public Object getValue() {
        return value;
    }

    public Entry changeValue(Object object) {
        return new Entry(slot, name, sqlType, object);
    }

    @Override
    public String toString() {
        return "{" + "name:" + name + ", sqlType:" + sqlType + ", value:" + value + '}';
    }

}
