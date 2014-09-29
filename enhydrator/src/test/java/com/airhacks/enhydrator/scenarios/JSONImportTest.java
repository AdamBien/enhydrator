package com.airhacks.enhydrator.scenarios;

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
import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.ScriptableSource;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class JSONImportTest {

    @Test
    public void readFromJSON() throws FileNotFoundException {
        VirtualSinkSource vss = getSource("./src/test/files/languages.json");
        int numberOfRows = vss.getNumberOfRows();
        assertThat(numberOfRows, is(1));
        Iterable<Row> query = vss.query(null);
        Iterator<Row> iterator = query.iterator();
        Row first = iterator.next();
        assertThat(first.getColumnValue("java"), is("1"));
        assertThat(first.getColumnValue("c"), is("2"));
        assertThat(first.getColumnValue("cobol"), is("3"));
        assertThat(first.getColumnValue("esoteric"), is("4"));

    }

    VirtualSinkSource getSource(final String fileName) throws FileNotFoundException {
        final Path pathToContent = Paths.get(fileName);
        Reader script = new FileReader("./src/test/files/converter.js");
        Source source = new ScriptableSource(pathToContent, script, "UTF-8");

        VirtualSinkSource vss = new VirtualSinkSource();
        Pump pump = new Pump.Engine().
                from(source).
                to(vss).
                to(new LogSink()).
                build();
        pump.start();
        return vss;
    }
}
