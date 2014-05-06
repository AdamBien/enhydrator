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
/**
 *
 * @author airhacks.com
 */
public class Column {

    private String name;
    private String destination;
    private Object value;
    private boolean nullValue;

    private final static String DEFAULT_DESTINATION = "*";

    public Column(String name, Object value) {
        this(name, DEFAULT_DESTINATION, value);
    }

    public Column(String name, String destination, Object value) {
        this.name = name;
        this.destination = destination;
        this.value = value;
    }

    public Column(String name) {
        this(name, null);
    }

    public boolean isNullValue() {
        return nullValue;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public String getDestination() {
        return destination;
    }

    void setName(String name) {
        this.name = name;
    }

    void setDestination(String destination) {
        this.destination = destination;
    }

    void setValue(Object value) {
        this.value = value;
    }

    void setNullValue(boolean nullValue) {
        this.nullValue = nullValue;
    }

    boolean isNumber() {
        return this.value instanceof Number;
    }

    boolean isString() {
        return this.value instanceof String;
    }

}
