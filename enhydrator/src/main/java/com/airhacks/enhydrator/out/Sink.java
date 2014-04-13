package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;

/**
 *
 * @author airhacks.com
 */
@FunctionalInterface
public interface Sink extends AutoCloseable {

    default void init() {
    }

    void processRow(List<Entry> entries);

    @Override
    default void close() {
    }
}
