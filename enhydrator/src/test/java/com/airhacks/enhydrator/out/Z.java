package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;

/**
 *
 * @author airhacks.com
 */
public class Z implements Sink {

    @Override
    public void processRow(List<Entry> entries) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
