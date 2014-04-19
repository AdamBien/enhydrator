package com.airhacks.enhydrator.transform;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
