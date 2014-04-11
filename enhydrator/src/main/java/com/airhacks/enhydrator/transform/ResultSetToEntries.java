package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 *
 * @author airhacks.com
 */
public class ResultSetToEntries implements Function<ResultSet, List<Entry>> {

    @Override
    public List<Entry> apply(ResultSet resultSet) {
        List<Entry> entries = new ArrayList<>();
        try {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                //from java.sql.Types
                int columnType = metaData.getColumnType(i);
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);
                Entry entry = new Entry(i - 1, columnName, columnType, value);
                entries.add(entry);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Problems accessing ResultSet", ex);
        }
        return entries;
    }

}
