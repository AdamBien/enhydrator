package com.airhacks.enhydrator.in;

import java.util.Optional;
import java.util.StringTokenizer;

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

    private int index;
    private String name;
    private String targetSink;
    private String targetObject;
    private Optional<Object> value;

    private final static String DEFAULT_DESTINATION = "*";

    public Column(int index, String name, Object value) {
        this(index, name, DEFAULT_DESTINATION, value);
    }

    public Column(int index, String name, String destination, Object value) {
        this.index = index;
        this.name = name;
        this.targetSink = destination;
        this.value = Optional.ofNullable(value);
    }

    public Column(int index, String name) {
        this(index, name, null);
    }

    public void convertToInteger() {
        if (value.isPresent()) {
            String asString = String.valueOf(value.get());
            try {
                this.value = Optional.of(Integer.parseInt(asString));
            } catch (NumberFormatException ex) {
                throw new NumberFormatException("Cannot convert column: "
                        + this.name + " with index " + this.index + " and value " + this.value + " to integer");
            }
        }
    }

    public void convertToDouble() {
        if (value.isPresent()) {
            String asString = String.valueOf(value.get());
            if (asString.isEmpty()) {
                this.value = Optional.of((double) 0);
                return;
            }
            try {
                this.value = Optional.of(Double.parseDouble(asString));
            } catch (NumberFormatException ex) {
                throw new NumberFormatException("Cannot convert column: "
                        + this.name + " with index " + this.index + " and value ->" + this.value + "<- to double");
            }
        }
    }

    public void convertDMSToDouble() {
        if (value.isPresent()) {

            String asString = String.valueOf(value.get());
            if (asString.isEmpty()) {
                this.value = Optional.of((double) 0);
                return;
            }
            StringTokenizer tokenizer = new StringTokenizer(asString, ".");
            int degree = 0, minute = 0, second = 0;
            if (tokenizer.hasMoreTokens()) {
                degree = Integer.parseInt(tokenizer.nextToken());
            }
            if (tokenizer.hasMoreTokens()) {
                minute = Integer.parseInt(tokenizer.nextToken());
            }
            if (tokenizer.hasMoreTokens()) {
                second = Integer.parseInt(tokenizer.nextToken());
            }
            this.value = Optional.of((double) degree + (minute / 60d) + (second / 3600d));
        }
    }

    public void convertToBoolean() {
        if (value.isPresent()) {

            String asString = String.valueOf(value.get());
            this.value = Optional.of(Boolean.parseBoolean(asString));
        }
    }

    public void fillWithValue(String value) {
        this.value = Optional.of(value);
    }

    public void convertToString() {
        if (value.isPresent()) {
            this.value = Optional.of(String.valueOf(value));
        }
    }

    public boolean isNullValue() {
        return !this.value.isPresent();
    }

    public String getName() {
        return name;
    }
 
    public Object getValue() {
        if(value.isPresent()) {
            return value.get();
        }
        return null;
    }

    public Optional<Object> getValueAsOptional() {
        return value;
    }

    public int getIndex() {
        return index;
    }

    public String getTargetSink() {
        return targetSink;
    }

    public String getTargetObject() {
        return targetObject;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTargetSink(String targetSink) {
        this.targetSink = targetSink;
    }

    public void setTargetObject(String targetObject) {
        this.targetObject = targetObject;
    }

    public void setValue(Object value) {
        this.value = Optional.of(value);
    }

    boolean isNumber() {
        return this.value.isPresent() && this.value.get() instanceof Number;
    }

    boolean isString() {
        return this.value.isPresent() && this.value.get() instanceof String;
    }

    @Override
    public String toString() {
        return "Column{" + "index=" + index + ", name=" + name + ", targetSink=" + targetSink + ", targetObject=" + targetObject + ", value=" + value.orElse("") + '}';
    }

}
