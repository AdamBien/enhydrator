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
import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 *
 * @author airhacks.com
 */
public class Entry {

    private int slot;
    private JsonObject value;
    private String destination = "*";
    private String name;

    public Entry(int slot, String name, JsonObject value) {
        this.slot = slot;
        this.value = value;
        this.name = name;
    }

    public Entry(int slot, String name, String value) {
        this(slot, name, toJson(name, value));
    }

    public Entry(int slot, String name, int value) {
        this(slot, name, toJson(name, value));
    }

    public Entry(int slot, String name, long value) {
        this(slot, name, toJson(name, value));
    }

    public Entry(int slot, String name, double value) {
        this(slot, name, toJson(name, value));
    }

    public Entry(int slot, String name, boolean value) {
        this(slot, name, toJson(name, value));
    }

    public static JsonObject toJson(String name, int value) {
        return Json.createObjectBuilder().add(name, value).build();
    }

    public static JsonObject toJson(String name, String value) {
        return Json.createObjectBuilder().add(name, value).build();
    }

    public static JsonObject toJson(String name, double value) {
        return Json.createObjectBuilder().add(name, value).build();
    }

    public static JsonObject toJson(String name, long value) {
        return Json.createObjectBuilder().add(name, value).build();
    }

    public static JsonObject toJson(String name, boolean value) {
        return Json.createObjectBuilder().add(name, value).build();
    }

    public int getSlot() {
        return slot;
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value.getString(this.name);
    }

    public String getDestination() {
        return destination;
    }

    public Entry changeValue(String value) {
        return new Entry(slot, name, value);
    }

    public boolean isNumber() {
        return JsonValue.ValueType.NUMBER == this.value.getValueType();
    }

    public boolean isString() {
        return JsonValue.ValueType.STRING == this.value.getValueType();
    }

    public Entry changeDestination(String destination) {
        Entry entry = new Entry(slot, name, value);
        entry.destination = destination;
        return entry;
    }

    public List<Entry> asList() {
        List<Entry> result = new ArrayList<>();
        result.add(this);
        return result;
    }
}
