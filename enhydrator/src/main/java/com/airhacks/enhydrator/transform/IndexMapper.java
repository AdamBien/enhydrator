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

import com.airhacks.enhydrator.flexpipe.RowTransformation;
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The IndexMapper maps the order of the columns based on the column headers in
 * the array list {@link IndexMapper#orderedNames}.
 *
 * @author ZeTo
 */
@XmlRootElement(name = "index-mapper")
@XmlAccessorType(XmlAccessType.FIELD)
public class IndexMapper extends RowTransformation {

    // A array list of column names that defines the indices of the columns.
    List<String> orderedNames;

    public IndexMapper() {
        orderedNames = new ArrayList<>();
    }

    @Override
    public Row execute(Row row) {
        if (row == null) {
            return null;
        }
        // reset all column indices to -1
        row.getColumns().stream().forEach(col -> col.setIndex(-1));
        // set indices according the array list
        this.orderedNames.stream().
                filter(f -> orderedNames.contains(f)).
                forEach(e -> setIndex(row.getColumnByName(e), orderedNames.indexOf(e)));
        // reindex the columns by index map in the row
        row.reindexColumns();
        return row;
    }

    void setIndex(Column column, int index) {
        if (column == null) {
            return;
        }
        column.setIndex(index);
    }
}
