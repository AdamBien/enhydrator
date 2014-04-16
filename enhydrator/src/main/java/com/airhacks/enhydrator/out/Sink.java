package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.flexpipe.JAXBInterfaceAdapter;
import com.airhacks.enhydrator.in.Entry;
import java.util.List;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 *
 * @author airhacks.com
 */
@XmlJavaTypeAdapter(JAXBInterfaceAdapter.class)
@FunctionalInterface
public interface Sink extends AutoCloseable {

    default void init() {
    }

    void processRow(List<Entry> entries);

    @Override
    default void close() {
    }
}
