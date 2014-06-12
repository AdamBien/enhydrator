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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author airhacks.com
 */
public class Memory {

    private final Map<String, Object> store;

    private LongAdder counter;
    private LongAdder processedRowCount;
    private LongAdder errorCount;

    public Memory() {
        this.store = new ConcurrentHashMap<>();
        this.counter = new LongAdder();
        this.processedRowCount = new LongAdder();
        this.errorCount = new LongAdder();
    }

    public Map<String, Object> put(String key, Object value) {
        this.store.put(key, value);
        return this.store;
    }

    public void increment() {
        counter.increment();
    }

    public void decrement() {
        counter.decrement();
    }

    public long counterValue() {
        return counter.longValue();
    }

    public Object get(String key) {
        return this.store.get(key);
    }

    public void processed() {
        this.processedRowCount.increment();
    }

    public void errorOccured() {
        this.errorCount.increment();
    }

    public long getProcessedRowCount() {
        return this.processedRowCount.longValue();
    }

    public long getNumberCount() {
        return this.errorCount.longValue();
    }

}
