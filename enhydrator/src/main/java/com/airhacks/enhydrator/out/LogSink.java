package com.airhacks.enhydrator.out;

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
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author airhacks.com
 */
public class LogSink extends Sink {

    private static final Logger LOG = Logger.getLogger(LogSink.class.getName());

    public LogSink(String name) {
        super(name);
    }

    public LogSink() {
    }

    @Override
    public void processRow(List<Entry> entries) {
        if (entries == null || entries.isEmpty()) {
            LOG.info("Empty list");
            return;
        }
        String row = entries.stream().map(e -> e.toString()).reduce((String t, String u) -> t + "," + u).get();
        LOG.info(row);
    }

}
