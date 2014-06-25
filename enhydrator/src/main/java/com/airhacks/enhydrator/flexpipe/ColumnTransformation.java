package com.airhacks.enhydrator.flexpipe;

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
import java.util.Objects;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "column-transformation")
public class ColumnTransformation {

    private String columnName;
    private Integer slot;
    private String function;

    public ColumnTransformation() {
    }

    public ColumnTransformation(String columnName, String function) {
        this.columnName = columnName;
        this.function = function;
    }

    public ColumnTransformation(int slot, String function) {
        this.slot = slot;
        this.function = function;
    }

    public String getColumnName() {
        return columnName;
    }

    public Integer getSlot() {
        return slot;
    }

    public String getFunction() {
        return function;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.columnName);
        hash = 53 * hash + Objects.hashCode(this.slot);
        hash = 53 * hash + Objects.hashCode(this.function);
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
        final ColumnTransformation other = (ColumnTransformation) obj;
        if (!Objects.equals(this.columnName, other.columnName)) {
            return false;
        }
        if (!Objects.equals(this.slot, other.slot)) {
            return false;
        }
        if (!Objects.equals(this.function, other.function)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ColumnTransformation{" + "columnName=" + columnName + ", slot=" + slot + ", function=" + function + '}';
    }

}
