package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;

/**
 *
 * @author airhacks.com
 */
public class SystemOutSink implements Sink {

    @Override
    public void processRow(List<Entry> entries) {
        System.out.println(entries);
    }

}
