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
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class VirtualSourceTest {

    @Test
    public void creation() {
        VirtualSource source = new VirtualSource.Rows().
                addColumn("age", "25").
                addColumn("master", "duke").
                addRow().
                addColumn("age", "15").
                addColumn("master", "juggy").
                build();

        assertThat(source.getNumberOfRows(), is(2));
        List<Entry> first = source.getRow(0);
        assertThat(first.size(), is(2));
        List<Entry> second = source.getRow(1);
        assertThat(second.size(), is(2));
    }

}
