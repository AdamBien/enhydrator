package com.airhacks.enhydrator.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 *
 * @author airhacks.com
 */
public class ResultSetIterator implements Iterator<ResultSet>, Iterable<ResultSet> {

    private final ResultSet resultSet;

    public ResultSetIterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        try {
            return this.resultSet.next();
        } catch (SQLException ex) {
            throw new IllegalStateException("ResultSet.next failed", ex);
        }
    }

    @Override
    public final ResultSet next() {
        return this.resultSet;
    }

    @Override
    public Iterator<ResultSet> iterator() {
        return this;
    }

}
