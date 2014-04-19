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

import java.sql.ResultSet;
import java.sql.SQLException;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author airhacks.com
 */
public class ResultSetIteratorTest {

    ResultSetIterator cut;
    ResultSet mockedResultSet;

    @Before
    public void init() {
        this.mockedResultSet = mock(ResultSet.class);
        this.cut = new ResultSetIterator(mockedResultSet);
    }

    @Test
    public void emptyResultSet() throws SQLException {
        when(this.mockedResultSet.next()).thenReturn(false);
        for (ResultSet resultSet : cut) {
            fail("Should not iterate over an empty resultset");
        }
    }

    @Test
    public void resultSet() throws SQLException {
        when(this.mockedResultSet.next()).thenReturn(true, false);
        boolean iterated = false;
        for (ResultSet resultSet : cut) {
            iterated = true;
        }
        assertTrue(iterated);
    }

}
