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
import com.airhacks.enhydrator.functions.NonRecursiveTree;
import com.airhacks.enhydrator.functions.SkipFirstRow;
import com.airhacks.enhydrator.in.CSVSource;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ParentTest {

    /**
     * Name;Size;Folder
     */
    @Test
    public void copy() {
        Source source = new CSVSource("./src/test/files/files.csv", ";", "UTF-8", true);
        VirtualSinkSource vss = new VirtualSinkSource();
        Pump pump = new Pump.Engine().
                from(source).
                startWith(new SkipFirstRow()).
                startWith(new NonRecursiveTree("Name", "Folder")).
                to(vss).
                to(new LogSink()).
                build();
        pump.start();
        System.out.println(vss.getRows());
        int numberOfRows = vss.getNumberOfRows();
        assertThat(numberOfRows, is(2));
        Row parentWithChildren = vss.getRow(0);
        assertNotNull(parentWithChildren);
        assertThat(parentWithChildren.getNumberOfColumns(), is(3));

        List<Row> children = parentWithChildren.getChildren();
        assertThat(children.size(), is(2));
        children.forEach(e -> assertThat(e.getNumberOfColumns(), is(2)));
    }

}
