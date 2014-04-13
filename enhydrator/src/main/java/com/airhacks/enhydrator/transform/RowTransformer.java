package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.List;

/**
 *
 * @author airhacks.com
 */
@FunctionalInterface
public interface RowTransformer {

    List<Entry> execute(List<Entry> input);

}
