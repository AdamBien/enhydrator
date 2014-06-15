package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Row;

/**
 *
 * @author airhacks.com
 */
public class TestRows {

    static Row getStringRow() {
        Row row = new Row();
        row.addColumn(0, "a", "java");
        row.addColumn(1, "b", "tengah");
        return row;
    }

    static Row getIntRow() {
        Row row = new Row();
        row.addColumn(0, "a", "1");
        row.addColumn(1, "b", "2");
        return row;
    }
}
