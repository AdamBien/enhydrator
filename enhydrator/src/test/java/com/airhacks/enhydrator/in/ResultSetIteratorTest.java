package com.airhacks.enhydrator.in;

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
