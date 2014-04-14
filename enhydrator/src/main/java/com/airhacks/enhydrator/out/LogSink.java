package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author airhacks.com
 */
public class LogSink implements Sink {

    private static final Logger LOG = Logger.getLogger(LogSink.class.getName());

    @Override
    public void processRow(List<Entry> entries) {
        String row = entries.stream().map(e -> e.toString()).reduce((String t, String u) -> t + "," + u).get();
        LOG.info(row);
    }

}
