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
import com.airhacks.enhydrator.in.Row;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "log-sink")
public class LogSink extends NamedSink {

    private static final Logger LOG = Logger.getLogger(LogSink.class.getName());

    public LogSink(String name) {
        super(name);
    }

    public LogSink() {
        super("*");
    }

    @Override
    public void processRow(Row entries) {
        if (entries == null || entries.isEmpty()) {
            LOG.info("Empty list");
            return;
        }
        String row = entries.getColumnsAsString().
                values().
                stream().
                reduce((String t, String u) -> t + "," + u).get();
        LOG.info(row);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LogSink;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        return hash;
    }

}
