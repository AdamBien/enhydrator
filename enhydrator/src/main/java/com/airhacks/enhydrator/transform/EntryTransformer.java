package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;

/**
 *
 * @author airhacks.com
 */
@FunctionalInterface
public interface EntryTransformer {

    List<Entry> execute(Entry entry, List result);

}
