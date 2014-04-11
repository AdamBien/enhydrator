package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 * @author airhacks.com
 */
public class SystemOutSink implements Consumer<List<Entry>> {

    @Override
    public void accept(List<Entry> t) {
        System.out.println(t);
    }

}
