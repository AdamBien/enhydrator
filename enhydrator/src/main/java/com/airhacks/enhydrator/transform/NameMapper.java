
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "name-mapper")
@XmlAccessorType(XmlAccessType.FIELD)
public class NameMapper extends RowTransformation {

    Map<String, String> mappings;

    public NameMapper() {
        this.mappings = new HashMap<>();
    }

    @Override
    public Row execute(Row row) {
        if (row == null) {
            return null;
        }
        this.mappings.entrySet().forEach(e -> changeName(row.getColumnByName(e.getKey()), e.getValue()));
        return row;
    }

    void changeName(Column column, String name) {
        if (column == null) {
            return;
        }
        column.setName(name);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 89 * hash + Objects.hashCode(this.mappings);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NameMapper other = (NameMapper)obj;
        if (!Objects.equals(this.mappings, other.mappings)) {
            return false;
        }
        return true;
    }


}
