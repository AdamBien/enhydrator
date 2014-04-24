package com.airhacks.enhydrator.in;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

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
    private String destination = "*";

    public Entry(int slot, String name, int sqlType, Object value) {
        this.slot = slot;
        this.name = name.toLowerCase();
        this.sqlType = sqlType;
        this.value = value;
    }

    public Entry(int slot, String name, String value) {
        this(slot, name, Types.VARCHAR, value);
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

    public String getDestination() {
        return destination;
    }

    public Entry changeValue(Object object) {
        return new Entry(slot, name, sqlType, object);
    }

    public Entry changeDestination(String destination) {
        Entry entry = new Entry(slot, name, sqlType, value);
        entry.destination = destination;
        return entry;
    }

    public List<Entry> asList() {
        List<Entry> result = new ArrayList<>();
        result.add(this);
        return result;
    }

    @Override
    public String toString() {
        return "{" + "name:" + name + ", sqlType:" + sqlType + ", value:" + value + '}';
    }

}
